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


def get_tmdb_movie_info(tmdb_id: int):
    """
    根据 tmdb_id 获取电影主海报 URL 和电影简介
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
    poster_url = f'{TMDB_IMAGE_BASE}{poster_path}' if poster_path else None

    overview = (data.get('overview') or '').strip()

    return {
        'poster_url': poster_url,
        'overview': overview,
    }


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
            # 如果海报和简介都有了，就跳过
            has_poster = bool(movie.poster and str(movie.poster).strip())
            has_overview = bool(movie.overview and str(movie.overview).strip())
            if has_poster and has_overview:
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

            movie_info = get_tmdb_movie_info(int(float(tmdb_id)))
            poster_url = movie_info.get('poster_url')
            overview = movie_info.get('overview')

            update_fields = []

            if poster_url and not has_poster:
                movie.poster = poster_url
                update_fields.append('poster')

            if overview and not has_overview:
                movie.overview = overview
                update_fields.append('overview')

            if not update_fields:
                print(f'[跳过] movieId={movie.id} 未找到可更新的信息')
                skipped_count += 1
                continue

            movie.save(update_fields=update_fields)

            updated_count += 1
            print(f'[成功] {movie.id} | {movie.title} | 更新字段: {", ".join(update_fields)}')

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