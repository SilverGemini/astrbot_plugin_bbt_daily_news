import asyncio
import datetime
import os
from typing import Dict, List, Any
import re
import traceback
import io
from dataclasses import dataclass
from datetime import timedelta

from PIL import Image as PILImage
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import aiohttp
import base64

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.core.message.components import Image
from astrbot.core.message.message_event_result import MessageChain

from apscheduler.schedulers.asyncio import AsyncIOScheduler

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# API配置常量
BANGUMI_CALENDAR_URL = "https://bgm.tv/calendar"
DMM_RANKING_URL = "https://www.dmm.co.jp/digital/videoa/-/ranking/=/term=daily/"

@dataclass
class CacheEntry:
    """缓存条目"""
    data: Any
    timestamp: datetime.datetime
    
    def is_expired(self, ttl_minutes: int = 10) -> bool:
        """检查缓存是否过期"""
        return datetime.datetime.now() > self.timestamp + timedelta(minutes=ttl_minutes)

@register("daily_report", "棒棒糖", "每日综合简报插件", "1.5.2")
class DailyReportPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config

        # 加载配置
        self.target_groups = config.get("target_groups", [])  # List[str]
        self.send_time = config.get("send_time", "08:00")  # HH:MM
        self.r18_mode = config.get("r18_mode", False)
        self.rawg_key = config.get("rawg_key", "")
        self.game_release_date_threshold = config.get("game_release_date_threshold", 14)
        self.report_jpeg_quality = config.get("report_jpeg_quality", 80)
        self.cache_ttl_minutes = config.get("cache_ttl_minutes", 10)  # 缓存有效时间，默认10分钟
        self.max_concurrent_requests = config.get("max_concurrent_requests", 5)
        
        # 初始化缓存
        self.cache = {}
        
        # 限制并发数
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        # 本地读取模板文件
        # 获取当前文件 (main.py) 所在的目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 拼接模板文件路径: group_summary/templates/report.html
        template_path = os.path.join(current_dir, "templates", "report.html")
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                self.html_template = f.read()
            logger.info(f"棒棒糖的每日晨报：成功加载模板: {template_path}")
        except FileNotFoundError:
            logger.error(f"棒棒糖的每日晨报：未找到模板文件: {template_path}")
            # 设置一个简单的兜底模板，防止崩溃
            self.html_template = "<h1>Template Not Found</h1>"

        # --- 修复部分：自己实例化调度器 ---
        self.scheduler = AsyncIOScheduler()

        # 解析时间
        try:
            hour, minute = self.send_time.split(":")
            # 添加定时任务
            self.scheduler.add_job(
                self.broadcast_report,
                'cron',
                hour=int(hour),
                minute=int(minute),
                id="daily_report_job"
            )
            # 启动调度器
            self.scheduler.start()
            logger.info(f"棒棒糖的每日晨报：定时任务已创建{self.send_time}")
        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：定时任务创建失败: {traceback.format_exc()}")
            logger.error(f"棒棒糖的每日晨报：定时任务创建失败: {e}")

    # --- 数据获取模块 ---

    async def fetch_bangumi_today(self, session) -> List[Dict]:
        """ 抓取今日番剧 (基于用户提供的层级优化)"""
        headers = {
            "User-Agent": user_agent
        }
        anime_list = []
        # Bangumi 页面使用英文简写作为 class 名
        weekday_map = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
        today_key = weekday_map[datetime.datetime.today().weekday()]

        try:
            async with self.semaphore:  # 限制并发
                async with session.get(BANGUMI_CALENDAR_URL, headers=headers) as resp:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'lxml')

                    # 策略：直接利用 class 名定位当天的数据，这比长 XPath 更稳定
                    # 对应你 XPath 中的 .../dl/dd 部分
                    day_section = soup.find("dd", class_=today_key)

                    if day_section:
                        # 对应你 XPath 中的 .../ul/li[...]
                        items = day_section.find_all("li")

                        for item in items:
                            data = {"title": "未知", "cover": ""}

                            # --- 1. 获取标题 ---
                            link_tag = item.find("a")
                            if link_tag:
                                title = link_tag.get_text(strip=True)
                                # 如果标题为空，直接跳过
                                if not title:
                                    continue
                                data["title"] = title

                            style_attr = item.get('style', '')
                            # --- 2. 获取图片 (双重策略) ---
                            url_match = re.search(r"url\('?(.*?)'?\)", style_attr)

                            img_url = "https://bgm.tv/img/no_icon_subject.png"
                            if url_match:
                                raw_url = url_match.group(1)
                                img_url = "https://" + raw_url.lstrip('/')

                            data["cover"] = img_url
                            anime_list.append(data)

        except asyncio.TimeoutError:
            logger.error("棒棒糖的每日晨报：获取今日番剧超时")
        except aiohttp.ClientError as e:
            logger.error(f"棒棒糖的每日晨报：获取今日番剧网络错误: {e}")
        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：抓取今日番剧失败: {e}")

        return anime_list

    async def fetch_dmm_top(self, session) -> List[Dict]:
        if not self.r18_mode:
            return []
        headers = {
            "User-Agent": user_agent
        }
        # 需要为cookies 设置 age_check_done=1，否则会返回年龄检查页面
        try:
            async with self.semaphore:  # 限制并发
                async with session.get(DMM_RANKING_URL, headers=headers, cookies={"age_check_done": "1"}) as resp:
                    # 获取网页内容文本
                    html_text = await resp.text()
                    #解析 HTML
                    soup = BeautifulSoup(html_text, 'lxml')
                    results = []

                    # 提取数据
                    # 逻辑：查找所有 id 以 "package-src-" 开头的 img 标签
                    targets = soup.find_all('img', id=re.compile(r'^package-src-'))

                    for img in targets:
                        title = img.get('alt')
                        src = img.get('src')

                        if title and src:
                            results.append({
                                "title": title,
                                "cover": src
                            })
                    return results
        except asyncio.TimeoutError:
            logger.error("棒棒糖的每日晨报：获取DMM数据超时")
        except aiohttp.ClientError as e:
            logger.error(f"棒棒糖的每日晨报：获取DMM数据网络错误: {e}")
        except Exception as e:
            logger.exception(f"棒棒糖的每日晨报：获取DMM数据失败: {e}")
        return []

    async def fetch_rawg_games(self, session) -> List[Dict]:
        if not self.rawg_key:
            return []

        # 计算日期范围：未来 {self.game_release_date_threshold} 天
        today = datetime.date.today()
        future = today + datetime.timedelta(days=self.game_release_date_threshold)
        dates_str = f"{today},{future}"

        # stores=1(Steam), 3(PlayStation Store), 6(Nintendo Store)
        # ordering=-released 表示发售日期倒序，无-表示正序
        url = f"https://api.rawg.io/api/games?key={self.rawg_key}&dates={dates_str}&stores=1,3,6&ordering=released&page_size=9"

        games_list = []
        try:
            async with self.semaphore:  # 限制并发
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results = data.get("results", [])

                        for item in results:
                            game = {}

                            # 1. 标题
                            game["title"] = item.get("name", "Unknown")

                            # 2. 封面 (转 Base64)
                            raw_bg = item.get("background_image", "")
                            if raw_bg:
                                # RAWG 图片支持裁剪参数，可以加 ?width=400 减小体积，但这里直接下原图也不大
                                game["cover"] = await self._url_to_base64(session, raw_bg, width=512)
                            else:
                                game["cover"] = ""

                            # 3. 平台信息
                            # 使用 parent_platforms 获取大类 (PC, PlayStation, Xbox, Nintendo)
                            platforms_data = item.get("parent_platforms", [])
                            p_names = []
                            if platforms_data:
                                for p_wrapper in platforms_data:
                                    p_info = p_wrapper.get("platform", {})
                                    p_name = p_info.get("name", "")
                                    if p_name == "PC":
                                        p_names.append("PC")
                                    elif p_name == "PlayStation":
                                        p_names.append("PlayStation")
                                    elif p_name == "Xbox":
                                        p_names.append("Xbox")
                                    elif p_name == "Nintendo":
                                        p_names.append("NS")
                                    elif p_name == "Apple Macintosh":
                                        p_names.append("Mac")
                                    else:
                                        p_names.append(p_name)

                            game["platforms"] = " / ".join(p_names) if p_names else "多平台"

                            # 4. 发售日期
                            game["release"] = item.get("released", "")[5:]  # 只取 MM-DD

                            games_list.append(game)

        except asyncio.TimeoutError:
            logger.error("棒棒糖的每日晨报：获取RAWG游戏数据超时")
        except aiohttp.ClientError as e:
            logger.error(f"棒棒糖的每日晨报：获取RAWG游戏数据网络错误: {e}")
        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：获取RAWG游戏数据失败: {e}")

        return games_list

    async def generate_html(self) -> Image:
        """聚合数据并渲染HTML，使用缓存机制"""
        # 尝试从缓存获取数据
        cache_key = "daily_report_data"
        cached_entry = self.cache.get(cache_key)
        
        if cached_entry and not cached_entry.is_expired(self.cache_ttl_minutes):
            logger.info("棒棒糖的每日晨报：使用缓存数据生成HTML")
            results_dict = cached_entry.data
        else:
            logger.info("棒棒糖的每日晨报：缓存未命中或已过期，开始获取最新数据")
            # 创建一个异步会话（不走代理）
            timeout = ClientTimeout(total=30)  # 明确设置总超时时间
            
            # 定义数据获取任务
            data_fetch_tasks = [
                self.fetch_bangumi_today,
                self.fetch_rawg_games,
            ]
            
            async with aiohttp.ClientSession(trust_env=False, timeout=timeout) as session:
                # 并发执行所有抓取任务
                logger.info("棒棒糖的每日晨报：开始并发获取数据")
                raw_results = await asyncio.gather(*[task(session) for task in data_fetch_tasks], return_exceptions=True)
            
            # 处理gather可能返回的异常对象
            results_dict = self._process_results(raw_results)
            
            # 将结果存入缓存
            self.cache[cache_key] = CacheEntry(data=results_dict, timestamp=datetime.datetime.now())
            logger.info("棒棒糖的每日晨报：数据已存入缓存")
        
        # 仅在启用R18模式时创建代理会话获取DMM数据
        dmm_top_list = []
        if self.r18_mode:
            # DMM数据单独处理，不放入缓存（因为它依赖于R18模式设置）
            timeout = ClientTimeout(total=30)
            async with aiohttp.ClientSession(trust_env=True, timeout=timeout) as session_proxy:
                dmm_results = await asyncio.gather(
                    self.fetch_dmm_top(session_proxy),
                    return_exceptions=True
                )
                dmm_top_list = dmm_results[0] if not isinstance(dmm_results[0], Exception) else []

        # 整理常规数据
        context_data = {
            "r18_mode": show_adult,
            "date": datetime.datetime.now().strftime("%Y-%m-%d %A"),
            "bangumi_list": results_dict['bangumi_today'],
            "dmm_top_list": dmm_top_list,
            "game_list": results_dict['rawg_games']
        }
        logger.info(f"棒棒糖的每日晨报：渲染数据: {context_data}")
        options = {"quality": self.report_jpeg_quality, "device_scale_factor_level": "ultra", "viewport_width": 505}
        img_result = await self.html_render(self.html_template, context_data, options=options)
        logger.info("棒棒糖的每日晨报：HTML 生成完成")
        return img_result

    def _process_results(self, raw_results):
        """处理原始结果，将其转换为字典格式"""
        # 定义结果映射
        result_mapping = {
            'bangumi_today': 0,
            'rawg_games': 1
        }
        
        results_dict = {}
        for key, index in result_mapping.items():
            result = raw_results[index]
            if isinstance(result, Exception):
                logger.error(f"数据获取任务 {key} 失败: {result}")
                # 根据任务类型返回默认值
                if key in ['bangumi_today']:
                    results_dict[key] = {"news": ["获取失败 - 网络错误"]}
                elif key in ['rawg_games']:
                        results_dict[key] = []
            else:
                results_dict[key] = result
                
        return results_dict

    async def broadcast_report(self):
        """定时任务入口"""
        logger.info("棒棒糖的每日晨报：开始每日晨报定时任务...")
        try:
            html_url = await self.generate_html()
            logger.info(f"棒棒糖的每日晨报：HTML 生成完成，路径地址: {html_url}")
            message_chain = MessageChain([Image.fromURL(html_url)])
            # 发送到配置的群
            for group_id in self.target_groups:
                logger.info(f"棒棒糖的每日晨报：向群组 {group_id} 发送图片")
                await self.context.send_message(group_id, message_chain)
                await asyncio.sleep(2)  # 防风控延迟

            logger.info("棒棒糖的每日晨报：每日报告广播完成。")

        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：广播失败: {e}", exc_info=True)

    async def _url_to_base64(self, session, url: str, referer: str = "", width: int = 0) -> str:
        """辅助方法：下载图片并转为 Base64 (支持本地缩放)"""
        if not url:
            return ""

        headers = {
            "User-Agent": user_agent
        }
        if referer:
            headers["Referer"] = referer

        try:
            async with self.semaphore:  # 限制并发
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        mime_type = resp.headers.get("Content-Type", "image/jpeg")

                        # --- 图片缩放逻辑 Start ---
                        if width > 0:
                            try:
                                # 将CPU密集型的PIL操作放到线程池中执行，避免阻塞事件循环
                                content = await asyncio.to_thread(
                                    self._resize_image_sync, 
                                    content, 
                                    width
                                )
                                mime_type = "image/jpeg"  # 缩放后统一转为 JPEG
                            except Exception as e:
                                logger.warning(f"棒棒糖的每日晨报：图片缩放失败 {url}: {e}")
                                # 缩放失败则使用原图，不中断流程
                        # --- 图片缩放逻辑 End ---

                        b64_str = base64.b64encode(content).decode("utf-8")
                        return f"data:{mime_type};base64,{b64_str}"
                    else:
                        logger.warning(f"棒棒糖的每日晨报：下载图片失败 {url}, 状态码: {resp.status}")
        except asyncio.TimeoutError:
            logger.warning(f"棒棒糖的每日晨报：图片下载超时 {url}")
        except aiohttp.ClientError as e:
            logger.warning(f"棒棒糖的每日晨报：图片下载网络错误 {url}: {e}")
        except Exception as e:
            logger.warning(f"棒棒糖的每日晨报：图片下载失败 {url}: {e}")

        return ""

    def _resize_image_sync(self, image_bytes: bytes, width: int) -> bytes:
        """同步的图片缩放操作，将在线程池中执行"""
        # 1. 打开图片
        img = PILImage.open(io.BytesIO(image_bytes))

        # 2. 计算缩放高度 (保持比例)
        w_percent = (width / float(img.size[0]))
        h_size = int((float(img.size[1]) * float(w_percent)))

        # 3. 执行缩放 (LANCZOS 滤镜质量最高)
        img = img.resize((width, h_size), PILImage.Resampling.LANCZOS)

        # 4. 保存回 bytes
        buffer = io.BytesIO()
        # 转换模式以适配 JPEG (如果是 PNG 带透明通道需转 RGB)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        img.save(buffer, format="JPEG", quality=95)  # 压缩质量 95
        return buffer.getvalue()
    
   
    # 也可以添加一个手动指令用于测试
    @filter.command("看看日报")
    async def manual_report(self, event: AstrMessageEvent):
        try:
            # 生成HTML图片
            html_url = await self.generate_html()
            logger.info("棒棒糖的每日晨报：手动报告生成成功")
            yield event.image_result(html_url)
        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：手动报告生成失败: {e}", exc_info=True)
            yield event.plain_result(f"生成报告失败: {str(e)}")

    @filter.command("清除日报缓存")
    async def clear_cache_command(self, event: AstrMessageEvent):
        """允许用户强制清除缓存"""
        self.cache.clear()
        logger.info("棒棒糖的每日晨报：缓存已被手动清除")
        yield event.plain_result("日报缓存已清除，下次查询将获取最新数据。")

    @filter.llm_tool(name="clear_daily_report_cache")
    async def tool_clear_cache(self, event: AstrMessageEvent):
        '''
        清理日报缓存


        '''
        self.cache.clear()
        logger.info("棒棒糖的每日晨报：缓存已被手动清除")
        return "日报缓存已清除，下次查询将获取最新数据。"


    @filter.llm_tool(name="today_news")
    async def report_today_news(self, event: AstrMessageEvent):
        """
        发送今天的早报，看看今天发生了什么。


        """
        try:
            html_url = await self.generate_html()
            logger.info("棒棒糖的每日晨报：LLM工具报告生成成功")
            yield event.image_result(html_url)
        except Exception as e:
            logger.error(f"棒棒糖的每日晨报：LLM工具报告生成失败: {e}", exc_info=True)
            yield event.plain_result(f"生成报告失败: {str(e)}")

    async def terminate(self):
        logger.info("棒棒糖的每日晨报：开始卸载...")
        if self.scheduler.running:
            self.scheduler.remove_all_jobs()
            self.scheduler.shutdown(wait=False)
        logger.info("棒棒糖的每日晨报：定时任务已清理")
        logger.info("棒棒糖的每日晨报：完成卸载...")
