import os
import time
import re
import requests
from urllib.parse import quote
from django.core.management.base import BaseCommand
from django.conf import settings
from recommend.models import Movie


class Command(BaseCommand):
    help = '从豆瓣获取电影海报并下载到本地（使用搜索建议接口）'

    def add_arguments(self, parser):
        parser.add_argument(
            '--cookie',
            type=str,
            default='',
            help='豆瓣登录Cookie（可选）'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=3.0,
            help='请求间隔（秒），建议3秒以上'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='每批处理的电影数量'
        )

    def handle(self, *args, **options):
        cookie_str = options['cookie']
        delay = options['delay']
        batch_size = options['batch_size']

        # 设置请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://movie.douban.com/',
        }
        if cookie_str:
            self.headers['Cookie'] = cookie_str

        # 创建海报存储目录
        poster_dir = os.path.join(settings.MEDIA_ROOT, 'posters')
        os.makedirs(poster_dir, exist_ok=True)

        # 筛选没有海报的电影
        movies_to_update = Movie.objects.filter(poster__isnull=True) | Movie.objects.filter(poster='')
        total = movies_to_update.count()
        self.stdout.write(f"发现 {total} 部电影需要更新海报")

        success_count = 0
        fail_count = 0
        skip_count = 0

        for idx, movie in enumerate(movies_to_update.iterator(chunk_size=batch_size)):
            # 清理标题：去除年份 (如 "Toy Story (1995)" -> "Toy Story")
            clean_title = re.sub(r'\s*\(\d{4}\)$', '', movie.title).strip()
            self.stdout.write(f"处理 [{idx + 1}/{total}] {movie.title} -> 搜索关键词: {clean_title}")

            # 1. 搜索电影获取豆瓣ID
            douban_id = self.search_movie_suggest(clean_title)
            if not douban_id:
                # 尝试用原标题再搜一次
                douban_id = self.search_movie_suggest(movie.title)

            if not douban_id:
                self.stdout.write(self.style.WARNING(f"  ⚠️ 未找到豆瓣ID: {movie.title}"))
                skip_count += 1
                time.sleep(delay)
                continue

            # 2. 获取高清海报URL
            poster_url = self.get_poster_url(douban_id)
            if not poster_url:
                self.stdout.write(self.style.WARNING(f"  ⚠️ 未找到海报: {movie.title}"))
                skip_count += 1
                time.sleep(delay)
                continue

            # 3. 下载海报到本地
            local_path = self.download_poster(poster_url, movie.id)
            if local_path:
                movie.poster = local_path
                movie.save(update_fields=['poster'])
                success_count += 1
                self.stdout.write(self.style.SUCCESS(f"  ✅ 成功: {movie.title}"))
            else:
                fail_count += 1
                self.stdout.write(self.style.ERROR(f"  ❌ 下载失败: {movie.title}"))

            # 请求间隔，避免被封
            time.sleep(delay)

        self.stdout.write(self.style.SUCCESS(
            f"完成！成功: {success_count}, 跳过: {skip_count}, 失败: {fail_count}"
        ))

    def search_movie_suggest(self, title):
        """使用豆瓣搜索建议接口获取电影ID"""
        try:
            url = f"https://movie.douban.com/j/subject_suggest?q={quote(title)}"
            resp = requests.get(url, headers=self.headers, timeout=10)

            if resp.status_code != 200:
                self.stdout.write(self.style.WARNING(f"  搜索建议接口返回状态码: {resp.status_code}"))
                return None

            data = resp.json()
            if not data:
                return None

            # 遍历结果，优先选择电影类型（episode=电影通常为0？）
            for item in data:
                # 检查类型是否为电影（subtype 可以是 'movie' 或 'tv'）
                if item.get('subtype') == 'movie':
                    return item.get('id')
            # 如果没有明确类型，返回第一个结果的id
            return data[0].get('id')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  搜索异常: {e}"))
            return None

    def get_poster_url(self, douban_id):
        """从电影详情页获取高清海报URL"""
        try:
            detail_url = f"https://movie.douban.com/subject/{douban_id}/"
            resp = requests.get(detail_url, headers=self.headers, timeout=10)

            if resp.status_code != 200:
                return None

            # 使用正则提取海报URL（避免解析复杂HTML）
            # 海报图片通常在 <img src="..." rel="v:image" /> 或 id="mainpic" 内
            import re
            match = re.search(r'<img[^>]+src="([^"]+)"[^>]+rel="v:image"', resp.text)
            if not match:
                match = re.search(r'<div id="mainpic"[^>]*>\s*<a[^>]+>\s*<img[^>]+src="([^"]+)"', resp.text)
            if not match:
                match = re.search(r'<img[^>]+src="([^"]+)"[^>]+title="点击看更多海报"', resp.text)

            if match:
                src = match.group(1)
                # 转换为大图
                src = src.replace('/s_ratio_poster', '/l').replace('/s_ratio_celebrity', '/l').replace('/webp/',
                                                                                                       '/jpg/')
                return src
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  获取海报URL异常: {e}"))
        return None

    def download_poster(self, url, movie_id):
        """下载海报到本地"""
        try:
            # 豆瓣图片有防盗链，需携带Referer
            headers = self.headers.copy()
            headers['Referer'] = 'https://movie.douban.com/'
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                file_path = os.path.join(settings.MEDIA_ROOT, 'posters', f'{movie_id}.jpg')
                with open(file_path, 'wb') as f:
                    f.write(resp.content)
                return f'/media/posters/{movie_id}.jpg'
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  下载图片异常: {e}"))
        return None