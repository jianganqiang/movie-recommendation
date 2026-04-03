import os
import time
from pathlib import Path

import django
import pandas as pd
import requests

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'movie_recommend.settings')
django.setup()

from recommend.models import Movie

BASE_DIR = Path(__file__).resolve().parent
LINKS_CSV = BASE_DIR / 'links.csv'

TMDB_TOKEN = os.getenv('TMDB_READ_ACCESS_TOKEN')
if not TMDB_TOKEN:
    raise ValueError('请先设置环境变量 TMDB_READ_ACCESS_TOKEN')

HEADERS = {
    'Authorization': f'Bearer {TMDB_TOKEN}',
    'accept': 'application/json',
}

TMDB_API_BASE = 'https://api.themoviedb.org/3'
TMDB_IMAGE_BASE = 'https://image.tmdb.org/t/p/w500'   # 前端展示足够用


def get_tmdb_poster_url(tmdb_id: int):
    """
    根据 tmdb_id 获取主海报 URL
    """
    url = f'{TMDB_API_BASE}/movie/{tmdb_id}'
    resp = requests.get(
        url,
        headers=HEADERS,
        params={'language': 'zh-CN'},
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    poster_path = data.get('poster_path')
    if not poster_path:
        return None

    return f'{TMDB_IMAGE_BASE}{poster_path}'


def get_tmdb_id_from_imdb(imdb_id: str):
    """
    当 links.csv 没有 tmdbId 时，尝试通过 imdbId 回查
    """
    if not imdb_id:
        return None

    imdb_str = str(imdb_id).strip()
    if not imdb_str:
        return None

    # MovieLens 里的 imdbId 通常没有 'tt' 前缀，这里补上
    if not imdb_str.startswith('tt'):
        imdb_str = 'tt' + imdb_str.zfill(7)

    url = f'{TMDB_API_BASE}/find/{imdb_str}'
    resp = requests.get(
        url,
        headers=HEADERS,
        params={'external_source': 'imdb_id'},
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    results = data.get('movie_results', [])
    if not results:
        return None

    return results[0].get('id')


def main():
    links_df = pd.read_csv(LINKS_CSV)

    # 建立 movieId -> {tmdbId, imdbId} 映射
    link_map = {
        int(row['movieId']): {
            'tmdbId': row['tmdbId'],
            'imdbId': row['imdbId'],
        }
        for _, row in links_df.iterrows()
    }

    updated_count = 0
    skipped_count = 0
    failed_count = 0

    movies = Movie.objects.all().iterator()

    for movie in movies:
        try:
            # 已经有海报就跳过；如果你想强制刷新，把这几行删掉
            if movie.poster:
                skipped_count += 1
                continue

            row = link_map.get(movie.id)
            if not row:
                print(f'[跳过] movieId={movie.id} 在 links.csv 中不存在')
                skipped_count += 1
                continue

            tmdb_id = row.get('tmdbId')
            imdb_id = row.get('imdbId')

            if pd.isna(tmdb_id):
                tmdb_id = get_tmdb_id_from_imdb(imdb_id)

            if not tmdb_id:
                print(f'[跳过] movieId={movie.id} 无可用 tmdbId')
                skipped_count += 1
                continue

            poster_url = get_tmdb_poster_url(int(float(tmdb_id)))
            if not poster_url:
                print(f'[跳过] movieId={movie.id} 未找到 poster_path')
                skipped_count += 1
                continue

            movie.poster = poster_url
            movie.save(update_fields=['poster'])

            updated_count += 1
            print(f'[成功] {movie.id} | {movie.title} -> {poster_url}')

            # 简单限速，避免请求太快
            time.sleep(0.15)

        except Exception as e:
            failed_count += 1
            print(f'[失败] movieId={movie.id}, title={movie.title}, error={e}')

    print('\n=== 完成 ===')
    print(f'更新成功: {updated_count}')
    print(f'跳过: {skipped_count}')
    print(f'失败: {failed_count}')


if __name__ == '__main__':
    main()