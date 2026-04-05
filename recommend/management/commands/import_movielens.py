import csv
from django.core.management.base import BaseCommand
from django.db import transaction
from recommend.models import Movie, Rating
from django.contrib.auth.models import User
from datetime import datetime


class Command(BaseCommand):
    help = '导入 MovieLens ml-latest-small 数据集（批量优化版）'

    def add_arguments(self, parser):
        parser.add_argument('movies_csv', type=str)
        parser.add_argument('ratings_csv', type=str)

    def handle(self, *args, **options):
        # ========== 1. 批量导入电影 ==========
        self.stdout.write('开始导入电影数据...')
        movie_data = []
        movie_ids = set()

        with open(options['movies_csv'], 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, 1):
                movie_id = int(row['movieId'])
                movie_data.append({
                    'id': movie_id,
                    'title': row['title'],
                    'genres': row['genres'],
                    'poster': ''
                })
                movie_ids.add(movie_id)

                if idx % 100 == 0:
                    self.stdout.write(f'已读取 {idx} 条电影数据')

        with transaction.atomic():
            for movie in movie_data:
                obj, created = Movie.objects.get_or_create(
                    id=movie['id'],
                    defaults={
                        'title': movie['title'],
                        'genres': movie['genres'],
                        'poster': movie['poster'],
                    }
                )

                if not created:
                    # 只更新基础字段，不覆盖 poster 和 overview
                    obj.title = movie['title']
                    obj.genres = movie['genres']
                    obj.save(update_fields=['title', 'genres'])

        self.stdout.write(self.style.SUCCESS('✅ 电影导入完成'))

        # ========== 2. 批量创建用户 ==========
        self.stdout.write('开始处理用户数据...')
        user_ids = set()
        rating_raw_data = []

        with open(options['ratings_csv'], 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, 1):
                user_id = int(row['userId'])
                movie_id = int(row['movieId'])

                if movie_id not in movie_ids:
                    continue

                user_ids.add(user_id)
                rating_raw_data.append({
                    'user_id': user_id,
                    'movie_id': movie_id,
                    'rating': float(row['rating']),
                    'timestamp': datetime.fromtimestamp(int(row['timestamp']))
                })

                if idx % 1000 == 0:
                    self.stdout.write(f'已读取 {idx} 条评分数据')

        users_to_create = []
        existing_users = User.objects.filter(username__in=[f'user{uid}' for uid in user_ids])
        existing_usernames = {u.username for u in existing_users}

        for uid in user_ids:
            username = f'user{uid}'
            if username not in existing_usernames:
                users_to_create.append(User(
                    username=username,
                    password='!',
                    id=uid
                ))

        if users_to_create:
            User.objects.bulk_create(users_to_create, batch_size=100)

        self.stdout.write(self.style.SUCCESS(f'✅ 用户处理完成，新增 {len(users_to_create)} 个用户'))

        # ========== 3. 批量导入评分 ==========
        self.stdout.write('开始批量导入评分...')
        ratings_to_create = []

        existing_ratings = Rating.objects.filter(
            user_id__in=user_ids,
            movie_id__in=movie_ids
        ).values_list('user_id', 'movie_id')
        existing_ratings_set = set(existing_ratings)

        for data in rating_raw_data:
            key = (data['user_id'], data['movie_id'])
            if key not in existing_ratings_set:
                ratings_to_create.append(Rating(
                    user_id=data['user_id'],
                    movie_id=data['movie_id'],
                    rating=data['rating'],
                    timestamp=data['timestamp']
                ))
            else:
                Rating.objects.filter(
                    user_id=data['user_id'],
                    movie_id=data['movie_id']
                ).update(
                    rating=data['rating'],
                    timestamp=data['timestamp']
                )

        with transaction.atomic():
            batch_size = 1000
            for i in range(0, len(ratings_to_create), batch_size):
                batch = ratings_to_create[i:i + batch_size]
                Rating.objects.bulk_create(batch, batch_size=batch_size)
                self.stdout.write(f'已导入 {min(i + batch_size, len(ratings_to_create))}/{len(ratings_to_create)} 条评分')

        self.stdout.write(self.style.SUCCESS(f'✅ 评分导入完成，新增 {len(ratings_to_create)} 条'))

        # ========== 4. 批量计算电影评分统计 ==========
        self.stdout.write('开始计算电影评分统计...')
        from django.db.models import Avg, Count

        movie_stats = Rating.objects.values('movie_id').annotate(
            avg_rating=Avg('rating'),
            rating_count=Count('id')
        )

        update_batches = []
        for stat in movie_stats:
            update_batches.append(Movie(
                id=stat['movie_id'],
                avg_rating=stat['avg_rating'],
                rating_count=stat['rating_count']
            ))

        Movie.objects.bulk_update(update_batches, ['avg_rating', 'rating_count'], batch_size=100)
        self.stdout.write(self.style.SUCCESS('✅ 电影评分统计更新完成'))

        self.stdout.write(self.style.SUCCESS('🎉 所有数据导入完成！'))