import os
import pickle
from collections import defaultdict

import pandas as pd
from django.conf import settings
from django.db.models import Case, When, IntegerField
from sklearn.metrics.pairwise import cosine_similarity

from .models import Movie, Rating


SIM_PATH = os.path.join(settings.BASE_DIR, 'item_sim.pkl')


def load_similarity():
    """加载预计算的电影相似度字典"""
    if not os.path.exists(SIM_PATH):
        return {}

    try:
        with open(SIM_PATH, 'rb') as f:
            data = pickle.load(f)
            return data or {}
    except Exception:
        return {}


def save_similarity(sim_dict):
    """保存电影相似度字典到本地文件"""
    with open(SIM_PATH, 'wb') as f:
        pickle.dump(sim_dict, f)


def compute_similarity(top_k=20):
    """离线计算电影相似度（基于评分矩阵）"""
    rows = list(Rating.objects.values('user_id', 'movie_id', 'rating'))
    if not rows:
        return {}

    df = pd.DataFrame(rows)
    if df.empty:
        return {}

    df['rating'] = df['rating'].astype(float)

    # 构建用户-物品矩阵
    pivot = df.pivot_table(
        index='user_id',
        columns='movie_id',
        values='rating',
        aggfunc='mean',
        fill_value=0
    )

    # 电影数量不足时无法计算有效相似度
    if pivot.shape[1] < 2:
        return {}

    # 计算物品间余弦相似度
    item_sim_matrix = cosine_similarity(pivot.T)
    item_sim_df = pd.DataFrame(
        item_sim_matrix,
        index=pivot.columns,
        columns=pivot.columns
    )

    top_sim = {}
    for movie_id in item_sim_df.columns:
        sim_scores = item_sim_df[movie_id].drop(labels=[movie_id], errors='ignore')
        sim_scores = sim_scores[sim_scores > 0].sort_values(ascending=False)

        top_sim[int(movie_id)] = {
            int(sim_movie_id): float(score)
            for sim_movie_id, score in sim_scores.head(top_k).items()
        }

    return top_sim


def rebuild_similarity_file(top_k=20):
    """重建并保存相似度文件"""
    sim_dict = compute_similarity(top_k=top_k)
    save_similarity(sim_dict)
    return sim_dict


def get_popular_movies(exclude_ids=None, top_n=10):
    """获取热门电影，支持排除已评分电影"""
    queryset = Movie.objects.all()
    if exclude_ids:
        queryset = queryset.exclude(id__in=list(exclude_ids))

    return queryset.order_by('-rating_count', '-avg_rating', 'id')[:top_n]


def get_recommendations(user, top_n=10):
    """为指定用户生成 Top-N 推荐"""
    user_ratings = Rating.objects.filter(user=user).select_related('movie')

    # 冷启动：用户还没评分
    if not user_ratings.exists():
        return get_popular_movies(top_n=top_n)

    rated_movies = {
        int(r.movie_id): float(r.rating)
        for r in user_ratings
    }

    sim_dict = load_similarity()

    # 相似度文件不存在时降级到热门推荐
    if not sim_dict:
        return get_popular_movies(exclude_ids=rated_movies.keys(), top_n=top_n)

    scores = defaultdict(float)

    for movie_id, rating in rated_movies.items():
        similar_movies = sim_dict.get(movie_id, {})
        for sim_movie_id, sim in similar_movies.items():
            sim_movie_id = int(sim_movie_id)

            # 跳过用户已评分电影
            if sim_movie_id in rated_movies:
                continue

            scores[sim_movie_id] += rating * float(sim)

    # 如果没有候选结果，退回热门推荐
    if not scores:
        return get_popular_movies(exclude_ids=rated_movies.keys(), top_n=top_n)

    sorted_movie_ids = [
        movie_id
        for movie_id, _ in sorted(
            scores.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True
        )[:top_n]
    ]

    # 关键修复：按推荐分数顺序返回，而不是被数据库打乱
    preserved_order = Case(
        *[When(id=movie_id, then=pos) for pos, movie_id in enumerate(sorted_movie_ids)],
        output_field=IntegerField()
    )

    return Movie.objects.filter(id__in=sorted_movie_ids).order_by(preserved_order)