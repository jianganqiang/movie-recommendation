import csv
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from django.core.management.base import BaseCommand
from django.conf import settings
from tmdbv3api import TMDb, Movie
from recommend.models import Movie as DjangoMovie


class Command(BaseCommand):
    help = '从 TMDb 获取电影海报 URL 并更新数据库（带超时和重试）'

    def add_arguments(self, parser):
        parser.add_argument(
            '--links-csv',
            type=str,
            default=os.path.join(settings.BASE_DIR, 'links.csv'),  # 默认放在项目根目录
            help='指向 links.csv 文件的路径'
        )
        parser.add_argument(
            '--api-key',
            type=str,
            default='78f08ea6f559ab44ecdb4e28416c9b2e',  # 请替换为你的真实 API 密钥
            help='TMDb API 密钥'
        )
        parser.add_argument(
            '--timeout',
            type=int,
            default=30,
            help='请求超时时间（秒）'
        )
        parser.add_argument(
            '--max-retries',
            type=int,
            default=5,
            help='最大重试次数'
        )
        parser.add_argument(
            '--retry-backoff',
            type=float,
            default=1.0,
            help='重试间隔因子（指数退避）'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='每批次处理的电影数量（用于迭代器）'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.5,
            help='请求之间的延迟时间（秒），避免请求过快'
        )

    def handle(self, *args, **options):
        links_csv_path = options['links_csv']
        api_key = options['api_key']
        timeout = options['timeout']
        max_retries = options['max_retries']
        retry_backoff = options['retry_backoff']
        batch_size = options['batch_size']
        delay = options['delay']

        self.stdout.write(f"正在读取 links 文件: {links_csv_path}")

        # 1. 读取 links.csv，建立 movieId -> tmdbId 映射
        movie_id_to_tmdb = {}
        try:
            with open(links_csv_path, mode='r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    movie_id = int(row['movieId'])
                    tmdb_id = row.get('tmdbId')
                    if tmdb_id and tmdb_id.strip():
                        movie_id_to_tmdb[movie_id] = int(tmdb_id.strip())
                    else:
                        movie_id_to_tmdb[movie_id] = None
            self.stdout.write(f"成功加载 {len(movie_id_to_tmdb)} 条 ID 映射。")
        except FileNotFoundError:
            self.stderr.write(self.style.ERROR(f"文件未找到: {links_csv_path}"))
            return

        # 2. 配置 TMDb 客户端
        tmdb = TMDb()
        tmdb.api_key = api_key
        tmdb.language = 'zh-CN'

        # 3. 创建带有重试机制的 requests session
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=retry_backoff,          # 重试间隔会逐渐增加：1, 2, 4, 8... 秒
            status_forcelist=[429, 500, 502, 503, 504],  # 遇到这些状态码时重试
            allowed_methods=["GET"]                 # 只对 GET 请求重试
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.timeout = timeout  # 设置全局超时（连接和读取）

        # 将 session 注入到 tmdb 库中（tmdbv3api 内部使用 requests）
        # 注意：tmdbv3api 的 TMDb 类有一个 _session 属性，我们可以直接替换
        tmdb._session = session

        # 初始化 Movie API
        tmdb_movie_api = Movie()

        # 4. 筛选需要更新的电影（poster 为空）
        movies_to_update = DjangoMovie.objects.filter(poster__isnull=True) | DjangoMovie.objects.filter(poster='')
        total_movies = movies_to_update.count()
        self.stdout.write(f"发现 {total_movies} 部电影需要更新海报。")

        updated_count = 0
        skipped_count = 0
        failed_count = 0

        # 5. 遍历并更新
        for django_movie in movies_to_update.iterator(chunk_size=batch_size):
            movie_id = django_movie.id
            tmdb_id = movie_id_to_tmdb.get(movie_id)

            if not tmdb_id:
                skipped_count += 1
                # self.stdout.write(self.style.WARNING(f"电影 ID {movie_id} 无 TMDb ID，跳过。"))
                continue

            try:
                # 通过 TMDb ID 获取电影详情
                tmdb_movie = tmdb_movie_api.details(tmdb_id)

                if hasattr(tmdb_movie, 'poster_path') and tmdb_movie.poster_path:
                    # 构建完整海报 URL（使用 w342 尺寸，可按需修改）
                    poster_url = f"https://image.tmdb.org/t/p/w342{tmdb_movie.poster_path}"

                    django_movie.poster = poster_url
                    django_movie.save(update_fields=['poster'])
                    updated_count += 1

                    if updated_count % 50 == 0:
                        self.stdout.write(f"已更新 {updated_count} 部电影...")
                else:
                    skipped_count += 1

                # 请求间隔，避免触发 API 限流
                time.sleep(delay)

            except Exception as e:
                self.stderr.write(self.style.ERROR(
                    f"处理电影 ID {movie_id} (TMDb ID: {tmdb_id}) 时出错: {e}"
                ))
                failed_count += 1
                # 出错后也等待一小段时间再继续
                time.sleep(delay * 2)

        # 6. 输出最终统计
        self.stdout.write(self.style.SUCCESS(
            f"任务完成。已更新: {updated_count}, 跳过: {skipped_count}, 失败: {failed_count}"
        ))