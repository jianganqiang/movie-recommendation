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
TMDB_IMAGE_BASE = 'https://image.tmdb.org/t/p/w500'


def get_tmdb_movie_info(tmdb_id: int):
    """
    根据 tmdb_id 获取电影信息：
    - 海报
    - 中文片名
    - 中文类型
    - 中文简介
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

    zh_title = (data.get('title') or '').strip()
    overview = (data.get('overview') or '').strip()

    genres_data = data.get('genres', [])
    zh_genres = '|'.join(
        g.get('name', '').strip()
        for g in genres_data
        if g.get('name')
    )

    return {
        'poster_url': poster_url,
        'zh_title': zh_title,
        'zh_genres': zh_genres,
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


def safe_str(value):
    """
    把 None 转成空字符串，方便判断
    """
    return str(value).strip() if value is not None else ''


def main():
    print('开始读取 links.csv ...')
    links_df = pd.read_csv(LINKS_CSV)

    # 建立 movieId -> {tmdbId, imdbId} 映射
    link_map = {
        int(row['movieId']): {
            'tmdbId': row['tmdbId'],
            'imdbId': row['imdbId'],
        }
        for _, row in links_df.iterrows()
    }

    movies_qs = Movie.objects.all().order_by('id')
    total = movies_qs.count()

    updated_count = 0
    skipped_count = 0
    failed_count = 0

    print(f'共需处理 {total} 部电影')
    print('开始更新...\n')

    for idx, movie in enumerate(movies_qs.iterator(chunk_size=200), 1):
        try:
            row = link_map.get(movie.id)
            if not row:
                skipped_count += 1
                print(f'[{idx}/{total}] [跳过] movieId={movie.id} | {movie.title} | links.csv 中不存在该电影')
                continue

            tmdb_id = row.get('tmdbId')
            imdb_id = row.get('imdbId')

            if pd.isna(tmdb_id):
                tmdb_id = get_tmdb_id_from_imdb(imdb_id)

            if not tmdb_id:
                skipped_count += 1
                print(f'[{idx}/{total}] [跳过] movieId={movie.id} | {movie.title} | 无可用 tmdbId')
                continue

            info = get_tmdb_movie_info(int(float(tmdb_id)))

            update_fields = []

            # poster：只在没有海报时更新
            if info.get('poster_url') and not safe_str(movie.poster):
                movie.poster = info['poster_url']
                update_fields.append('poster')

            # zh_title：有内容就更新
            if info.get('zh_title') and safe_str(getattr(movie, 'zh_title', '')) != info['zh_title']:
                movie.zh_title = info['zh_title']
                update_fields.append('zh_title')

            # zh_genres：有内容就更新
            if info.get('zh_genres') and safe_str(getattr(movie, 'zh_genres', '')) != info['zh_genres']:
                movie.zh_genres = info['zh_genres']
                update_fields.append('zh_genres')

            # overview：只在没有简介时更新
            if info.get('overview') and not safe_str(getattr(movie, 'overview', '')):
                movie.overview = info['overview']
                update_fields.append('overview')

            if not update_fields:
                skipped_count += 1
                print(f'[{idx}/{total}] [跳过] movieId={movie.id} | {movie.title} | 无需更新')
                continue

            movie.save(update_fields=update_fields)
            updated_count += 1

            print(
                f'[{idx}/{total}] [成功] movieId={movie.id} | {movie.title} | '
                f'更新字段: {", ".join(update_fields)}'
            )

            # 简单限速，避免请求过快
            time.sleep(0.15)

        except Exception as e:
            failed_count += 1
            print(
                f'[{idx}/{total}] [失败] movieId={movie.id} | {movie.title} | error={e}'
            )
            time.sleep(0.15)

    print('\n=== 完成 ===')
    print(f'总数: {total}')
    print(f'更新成功: {updated_count}')
    print(f'跳过: {skipped_count}')
    print(f'失败: {failed_count}')


if __name__ == '__main__':
    main()