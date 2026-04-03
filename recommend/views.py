from decimal import Decimal

from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Avg, Count
from django.shortcuts import render, get_object_or_404, redirect

from .forms import RegisterForm, LoginForm, RatingForm
from .models import Movie, Rating
from .utils import get_recommendations, rebuild_similarity_file


def update_movie_stats(movie):
    """更新单部电影的平均分和评分人数"""
    stats = Rating.objects.filter(movie=movie).aggregate(
        avg_rating=Avg('rating'),
        rating_count=Count('id')
    )
    movie.avg_rating = float(stats['avg_rating'] or 0)
    movie.rating_count = stats['rating_count'] or 0
    movie.save(update_fields=['avg_rating', 'rating_count'])


def index(request):
    """首页：已登录用户显示推荐，否则显示热门电影"""
    if request.user.is_authenticated:
        movies = get_recommendations(request.user, top_n=10)
    else:
        movies = Movie.objects.order_by('-rating_count', '-avg_rating', 'id')[:10]

    return render(request, 'recommend/index.html', {'movies': movies})


def register_view(request):
    if request.method == 'POST':
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('index')
    else:
        form = RegisterForm()

    return render(request, 'recommend/register.html', {'form': form})


def login_view(request):
    if request.method == 'POST':
        form = LoginForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            return redirect('index')
    else:
        form = LoginForm()

    return render(request, 'recommend/login.html', {'form': form})


def logout_view(request):
    logout(request)
    return redirect('index')


@login_required
def profile_view(request):
    user_ratings = Rating.objects.filter(user=request.user).select_related('movie').order_by('-timestamp')
    return render(request, 'recommend/profile.html', {'ratings': user_ratings})


def movie_list(request):
    movies = Movie.objects.all()

    query = request.GET.get('q', '').strip()
    if query:
        movies = movies.filter(title__icontains=query)

    genre = request.GET.get('genre', '').strip()
    if genre:
        movies = movies.filter(genres__icontains=genre)

    paginator = Paginator(movies.order_by('id'), 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # 只取 genres 字段，减少不必要的对象加载
    genre_strings = Movie.objects.values_list('genres', flat=True)
    all_genres = sorted({
        g.strip()
        for genres in genre_strings if genres
        for g in genres.split('|')
        if g.strip()
    })

    return render(request, 'recommend/movie_list.html', {
        'page_obj': page_obj,
        'query': query,
        'selected_genre': genre,
        'all_genres': all_genres,
    })


def movie_detail(request, pk):
    movie = get_object_or_404(Movie, pk=pk)
    user_rating = None

    if request.user.is_authenticated:
        user_rating = Rating.objects.filter(user=request.user, movie=movie).first()

    if request.method == 'POST':
        if not request.user.is_authenticated:
            return redirect('login')

        form = RatingForm(request.POST)
        if form.is_valid():
            rating_value = Decimal(form.cleaned_data['rating'])

            Rating.objects.update_or_create(
                user=request.user,
                movie=movie,
                defaults={'rating': rating_value}
            )

            update_movie_stats(movie)

            # 小型课程项目可直接重建；若后续数据量增大，可改成离线任务
            rebuild_similarity_file()

            return redirect('movie_detail', pk=pk)
    else:
        form = RatingForm(
            initial={'rating': str(user_rating.rating) if user_rating else '3.0'}
        )

    return render(request, 'recommend/movie_detail.html', {
        'movie': movie,
        'user_rating': user_rating,
        'form': form,
    })


@login_required
def delete_rating(request, pk):
    movie = get_object_or_404(Movie, pk=pk)
    Rating.objects.filter(user=request.user, movie=movie).delete()

    update_movie_stats(movie)

    # 小型课程项目可直接重建；若后续数据量增大，可改成离线任务
    rebuild_similarity_file()

    return redirect('movie_detail', pk=pk)