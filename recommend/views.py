import os
import pickle
from collections import defaultdict

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Avg, Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from .forms import LoginForm, RatingForm, RegisterForm
from .models import Movie, Rating


SIM_PATH = os.path.join(settings.BASE_DIR, 'item_sim.pkl')


def get_all_genres():
    """
    提取数据库中所有电影类型，供列表页筛选使用
    """
    genres_set = set()
    for genres_str in Movie.objects.exclude(genres__isnull=True).exclude(genres='').values_list('genres', flat=True):
        for genre in str(genres_str).split('|'):
            genre = genre.strip()
            if genre and genre != '(no genres listed)':
                genres_set.add(genre)
    return sorted(genres_set)


def get_movie_genres(movie):
    """
    将电影的 genres 字符串拆成列表
    """
    if not movie.genres:
        return []

    genres = []
    for genre in str(movie.genres).split('|'):
        genre = genre.strip()
        if genre and genre != '(no genres listed)':
            genres.append(genre)
    return genres


def refresh_movie_rating_stats(movie):
    """
    更新电影平均分和评分人数
    """
    stats = Rating.objects.filter(movie=movie).aggregate(
        avg_rating=Avg('rating'),
        rating_count=Count('id')
    )

    movie.avg_rating = float(stats['avg_rating']) if stats['avg_rating'] is not None else 0.0
    movie.rating_count = stats['rating_count'] or 0
    movie.save(update_fields=['avg_rating', 'rating_count'])


def get_hot_movies(limit=12):
    """
    热门电影：按评分人数和平均分排序
    """
    return Movie.objects.order_by('-rating_count', '-avg_rating', 'title')[:limit]


def load_similarity_dict():
    """
    读取 item-item 相似度字典
    """
    if not os.path.exists(SIM_PATH):
        return {}

    try:
        with open(SIM_PATH, 'rb') as f:
            sim_dict = pickle.load(f)
        return sim_dict if isinstance(sim_dict, dict) else {}
    except Exception:
        return {}


def get_similarity_recommendations(user, limit=12):
    """
    基于 item_sim.pkl 的个性化推荐
    """
    user_ratings = Rating.objects.filter(user=user).select_related('movie')
    if not user_ratings.exists():
        return []

    sim_dict = load_similarity_dict()
    if not sim_dict:
        return []

    rated_movie_ids = set()
    scores = defaultdict(float)

    for rating_obj in user_ratings:
        movie_id = rating_obj.movie_id
        rating_value = float(rating_obj.rating)
        rated_movie_ids.add(movie_id)

        similar_movies = sim_dict.get(movie_id, {})
        for sim_movie_id, sim_score in similar_movies.items():
            if sim_movie_id in rated_movie_ids:
                continue
            scores[int(sim_movie_id)] += rating_value * float(sim_score)

    if not scores:
        return []

    ranked_ids = [
        movie_id
        for movie_id, _ in sorted(scores.items(), key=lambda x: (x[1], x[0]), reverse=True)
    ]

    movies_map = Movie.objects.in_bulk(ranked_ids)
    result = []

    for movie_id in ranked_ids:
        movie = movies_map.get(movie_id)
        if movie:
            result.append(movie)
        if len(result) >= limit:
            break

    return result


def get_genre_based_recommendations(user, limit=12):
    """
    当相似度推荐不可用时，按用户高分电影偏好类型推荐
    """
    user_ratings = Rating.objects.filter(user=user).select_related('movie')
    if not user_ratings.exists():
        return list(get_hot_movies(limit))

    genre_scores = defaultdict(float)
    rated_movie_ids = set()

    for rating_obj in user_ratings:
        rated_movie_ids.add(rating_obj.movie_id)

        # 评分越高，类型偏好权重越大
        weight = max(float(rating_obj.rating) - 2.5, 0)
        if weight <= 0:
            continue

        for genre in get_movie_genres(rating_obj.movie):
            genre_scores[genre] += weight

    if not genre_scores:
        return list(
            Movie.objects.exclude(id__in=rated_movie_ids).order_by('-rating_count', '-avg_rating', 'title')[:limit]
        )

    candidates = Movie.objects.exclude(id__in=rated_movie_ids).order_by('-rating_count', '-avg_rating', 'title')[:400]

    scored_movies = []
    for movie in candidates:
        score = 0.0
        for genre in get_movie_genres(movie):
            score += genre_scores.get(genre, 0)
        if score > 0:
            scored_movies.append((movie, score))

    scored_movies.sort(key=lambda x: (x[1], x[0].rating_count, x[0].avg_rating), reverse=True)

    result = [movie for movie, _ in scored_movies[:limit]]
    if len(result) < limit:
        existing_ids = {movie.id for movie in result}
        fallback_movies = Movie.objects.exclude(id__in=rated_movie_ids | existing_ids).order_by(
            '-rating_count', '-avg_rating', 'title'
        )[:limit - len(result)]
        result.extend(list(fallback_movies))

    return result


def get_personalized_movies(user, limit=12):
    """
    统一个性化推荐入口：
    1. 优先 item_sim.pkl
    2. 不行就按类型偏好
    3. 还不行就热门电影
    """
    movies = get_similarity_recommendations(user, limit=limit)
    if movies:
        return movies

    movies = get_genre_based_recommendations(user, limit=limit)
    if movies:
        return movies

    return list(get_hot_movies(limit))


def index(request):
    """
    首页：
    - 已登录：个性化推荐
    - 未登录：热门电影
    """
    if request.user.is_authenticated:
        movies = get_personalized_movies(request.user, limit=12)
    else:
        movies = list(get_hot_movies(12))

    return render(request, 'recommend/index.html', {
        'movies': movies
    })


def register_view(request):
    if request.user.is_authenticated:
        return redirect('index')

    if request.method == 'POST':
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, '注册成功，已自动登录。')
            return redirect('index')
    else:
        form = RegisterForm()

    return render(request, 'recommend/register.html', {'form': form})


def login_view(request):
    if request.user.is_authenticated:
        return redirect('index')

    if request.method == 'POST':
        form = LoginForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            messages.success(request, '登录成功。')
            return redirect('index')
    else:
        form = LoginForm(request)

    return render(request, 'recommend/login.html', {'form': form})


def logout_view(request):
    if request.method == 'POST':
        logout(request)
        messages.success(request, '你已退出登录。')
    return redirect('index')


@login_required
def profile_view(request):
    ratings = Rating.objects.filter(user=request.user).select_related('movie').order_by('-timestamp')

    return render(request, 'recommend/profile.html', {
        'ratings': ratings
    })


def movie_list(request):
    """
    电影列表页：支持关键字搜索、类型筛选、分页
    """
    query = request.GET.get('q', '').strip()
    selected_genre = request.GET.get('genre', '').strip()

    movies = Movie.objects.all()

    if query:
        movies = movies.filter(title__icontains=query)

    if selected_genre:
        movies = movies.filter(genres__icontains=selected_genre)

    movies = movies.order_by('-rating_count', '-avg_rating', 'title')

    paginator = Paginator(movies, 24)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'recommend/movie_list.html', {
        'page_obj': page_obj,
        'query': query,
        'selected_genre': selected_genre,
        'all_genres': get_all_genres(),
    })


def movie_detail(request, pk):
    """
    电影详情页：
    - GET：展示详情和评分表单
    - POST：提交/更新当前用户评分
    """
    movie = get_object_or_404(Movie, pk=pk)
    user_rating = None

    if request.user.is_authenticated:
        user_rating = Rating.objects.filter(user=request.user, movie=movie).first()

    if request.method == 'POST':
        if not request.user.is_authenticated:
            return redirect('login')

        form = RatingForm(request.POST)
        if form.is_valid():
            rating_value = form.cleaned_data['rating']

            if user_rating:
                user_rating.rating = rating_value
                user_rating.timestamp = timezone.now()
                user_rating.save(update_fields=['rating', 'timestamp'])
                messages.success(request, '评分更新成功。')
            else:
                Rating.objects.create(
                    user=request.user,
                    movie=movie,
                    rating=rating_value
                )
                messages.success(request, '评分提交成功。')

            refresh_movie_rating_stats(movie)
            return redirect('movie_detail', pk=movie.pk)
    else:
        initial_data = {'rating': str(user_rating.rating)} if user_rating else None
        form = RatingForm(initial=initial_data)

    return render(request, 'recommend/movie_detail.html', {
        'movie': movie,
        'form': form,
        'user_rating': user_rating,
    })


@login_required
def delete_rating(request, pk):
    if request.method != 'POST':
        return redirect('movie_detail', pk=pk)

    movie = get_object_or_404(Movie, pk=pk)
    rating = Rating.objects.filter(user=request.user, movie=movie).first()

    if rating:
        rating.delete()
        refresh_movie_rating_stats(movie)
        messages.success(request, '评分已删除。')
    else:
        messages.warning(request, '未找到你的评分记录。')

    return redirect('movie_detail', pk=movie.pk)