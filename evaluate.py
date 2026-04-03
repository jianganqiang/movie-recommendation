import os
import math
import random
from collections import defaultdict

import django
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'movie_recommend.settings')
django.setup()

from recommend.models import Rating


def load_ratings_from_db(min_positive_rating=4):
    """
    从数据库读取评分数据，只保留正反馈（rating >= min_positive_rating）
    """
    rows = list(Rating.objects.values('user_id', 'movie_id', 'rating'))
    if not rows:
        return pd.DataFrame(columns=['user_id', 'movie_id', 'rating'])

    df = pd.DataFrame(rows)
    df['rating'] = df['rating'].astype(float)

    # Top-N 推荐评估里通常把高分视为“用户喜欢”
    df = df[df['rating'] >= min_positive_rating].copy()
    return df


def leave_one_out_split(df, min_user_interactions=2, seed=42):
    """
    对每个用户做 leave-one-out：
    - 测试集：随机留出 1 个正反馈电影
    - 训练集：其余正反馈电影
    """
    random.seed(seed)

    train_rows = []
    test_items = {}  # user_id -> held-out movie_id

    for user_id, group in df.groupby('user_id'):
        items = group['movie_id'].tolist()

        if len(items) < min_user_interactions:
            continue

        test_item = random.choice(items)
        test_items[user_id] = test_item

        for _, row in group.iterrows():
            if row['movie_id'] != test_item:
                train_rows.append(row.to_dict())

    train_df = pd.DataFrame(train_rows)
    return train_df, test_items


def build_item_similarity(train_df, top_k_sim=20):
    """
    根据训练集构建 item-item 相似度字典
    """
    if train_df.empty:
        return {}

    pivot = train_df.pivot_table(
        index='user_id',
        columns='movie_id',
        values='rating',
        aggfunc='mean',
        fill_value=0
    )

    if pivot.shape[1] < 2:
        return {}

    item_sim_matrix = cosine_similarity(pivot.T)
    item_sim_df = pd.DataFrame(
        item_sim_matrix,
        index=pivot.columns,
        columns=pivot.columns
    )

    sim_dict = {}
    for movie_id in item_sim_df.columns:
        sim_scores = item_sim_df[movie_id].drop(labels=[movie_id], errors='ignore')
        sim_scores = sim_scores[sim_scores > 0].sort_values(ascending=False)

        sim_dict[int(movie_id)] = {
            int(sim_movie_id): float(score)
            for sim_movie_id, score in sim_scores.head(top_k_sim).items()
        }

    return sim_dict


def build_user_histories(train_df):
    """
    构建训练集里的用户历史：user_id -> {movie_id: rating}
    """
    user_histories = defaultdict(dict)
    for _, row in train_df.iterrows():
        user_histories[int(row['user_id'])][int(row['movie_id'])] = float(row['rating'])
    return user_histories


def recommend_for_user(user_history, sim_dict, top_k=10):
    """
    对单个用户生成 Top-K 推荐
    """
    scores = defaultdict(float)

    for movie_id, rating in user_history.items():
        similar_movies = sim_dict.get(movie_id, {})
        for sim_movie_id, sim in similar_movies.items():
            if sim_movie_id in user_history:
                continue
            scores[sim_movie_id] += rating * sim

    ranked = sorted(scores.items(), key=lambda x: (x[1], x[0]), reverse=True)
    return [movie_id for movie_id, _ in ranked[:top_k]]


def evaluate_topk(user_histories, test_items, sim_dict, k=10):
    """
    计算 Precision@K / Recall@K / HitRate@K / NDCG@K
    每个用户只有 1 个测试正样本（leave-one-out）
    """
    if not user_histories or not test_items:
        return None

    precisions = []
    recalls = []
    hits = []
    ndcgs = []

    evaluated_users = 0

    for user_id, true_item in test_items.items():
        if user_id not in user_histories:
            continue

        recs = recommend_for_user(user_histories[user_id], sim_dict, top_k=k)
        if not recs:
            continue

        evaluated_users += 1

        if true_item in recs:
            rank = recs.index(true_item) + 1
            hit = 1
            precision = 1 / k
            recall = 1.0
            ndcg = 1 / math.log2(rank + 1)
        else:
            hit = 0
            precision = 0.0
            recall = 0.0
            ndcg = 0.0

        hits.append(hit)
        precisions.append(precision)
        recalls.append(recall)
        ndcgs.append(ndcg)

    if evaluated_users == 0:
        return None

    return {
        'evaluated_users': evaluated_users,
        'Precision@K': sum(precisions) / len(precisions),
        'Recall@K': sum(recalls) / len(recalls),
        'HitRate@K': sum(hits) / len(hits),
        'NDCG@K': sum(ndcgs) / len(ndcgs),
    }


def main():
    K = 20
    MIN_POSITIVE_RATING = 3
    MIN_USER_INTERACTIONS = 2
    TOP_K_SIM = 50

    print('开始读取评分数据...')
    df = load_ratings_from_db(min_positive_rating=MIN_POSITIVE_RATING)

    if df.empty:
        print('没有可用评分数据，无法评估。')
        return

    print(f'正反馈评分数: {len(df)}')
    print(f'用户数: {df["user_id"].nunique()}')
    print(f'电影数: {df["movie_id"].nunique()}')

    print('\n开始划分训练集 / 测试集...')
    train_df, test_items = leave_one_out_split(
        df,
        min_user_interactions=MIN_USER_INTERACTIONS,
        seed=42
    )

    if train_df.empty or not test_items:
        print('训练/测试划分失败，可能是有效用户太少。')
        return

    print(f'训练样本数: {len(train_df)}')
    print(f'测试用户数: {len(test_items)}')

    print('\n开始计算电影相似度...')
    sim_dict = build_item_similarity(train_df, top_k_sim=TOP_K_SIM)

    if not sim_dict:
        print('相似度矩阵为空，无法评估。')
        return

    print('开始生成推荐并计算指标...')
    user_histories = build_user_histories(train_df)
    metrics = evaluate_topk(user_histories, test_items, sim_dict, k=K)

    if not metrics:
        print('没有成功评估到任何用户。')
        return

    print('\n===== 评估结果 =====')
    print(f'评估用户数: {metrics["evaluated_users"]}')
    print(f'Precision@{K}: {metrics["Precision@K"]:.4f}')
    print(f'Recall@{K}:    {metrics["Recall@K"]:.4f}')
    print(f'HitRate@{K}:   {metrics["HitRate@K"]:.4f}')
    print(f'NDCG@{K}:      {metrics["NDCG@K"]:.4f}')


if __name__ == '__main__':
    main()