from django.contrib import admin
from .models import Movie, Rating


def field_exists(model, field_name):
    """
    判断模型字段是否存在
    """
    return field_name in {f.name for f in model._meta.get_fields()}


def get_model_field_names(model):
    return {f.name for f in model._meta.get_fields()}


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_per_page = 20
    ordering = ['id']

    def get_queryset(self, request):
        return super().get_queryset(request)

    @admin.display(description='电影ID', ordering='id')
    def movie_id(self, obj):
        return obj.id

    @admin.display(description='电影名称')
    def display_title(self, obj):
        zh_title = getattr(obj, 'zh_title', None)
        return zh_title or obj.title

    @admin.display(description='电影类型')
    def display_genres(self, obj):
        zh_genres = getattr(obj, 'zh_genres', None)
        return zh_genres or obj.genres

    @admin.display(description='简介摘要')
    def short_overview(self, obj):
        overview = getattr(obj, 'overview', '') or ''
        if not overview:
            return '暂无简介'
        return overview[:40] + ('...' if len(overview) > 40 else '')

    @admin.display(description='平均评分', ordering='avg_rating')
    def display_avg_rating(self, obj):
        return f'{obj.avg_rating:.1f}'

    @admin.display(description='评分人数', ordering='rating_count')
    def display_rating_count(self, obj):
        return obj.rating_count

    def get_list_display(self, request):
        fields = [
            'movie_id',
            'display_title',
            'display_genres',
        ]

        if field_exists(Movie, 'overview'):
            fields.append('short_overview')

        fields.extend([
            'display_avg_rating',
            'display_rating_count',
        ])
        return fields

    def get_search_fields(self, request):
        candidates = ['title', 'genres']
        if field_exists(Movie, 'zh_title'):
            candidates.append('zh_title')
        if field_exists(Movie, 'zh_genres'):
            candidates.append('zh_genres')
        if field_exists(Movie, 'overview'):
            candidates.append('overview')
        return candidates

    def get_list_filter(self, request):
        filters = []
        if field_exists(Movie, 'genres'):
            filters.append('genres')
        if field_exists(Movie, 'zh_genres'):
            filters.append('zh_genres')
        return filters

    def get_readonly_fields(self, request, obj=None):
        readonly = ['id', 'avg_rating', 'rating_count']
        return [f for f in readonly if field_exists(Movie, f)]

    def get_fields(self, request, obj=None):
        candidates = [
            'id',
            'title',
            'zh_title',
            'genres',
            'zh_genres',
            'poster',
            'overview',
            'avg_rating',
            'rating_count',
        ]
        existing = get_model_field_names(Movie)
        return [f for f in candidates if f in existing]


@admin.register(Rating)
class RatingAdmin(admin.ModelAdmin):
    list_per_page = 30
    ordering = ['-timestamp']
    list_filter = ['rating', 'timestamp']
    search_fields = ['user__username', 'movie__title']

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user', 'movie')

    @admin.display(description='用户', ordering='user__username')
    def display_user(self, obj):
        return obj.user.username

    @admin.display(description='电影名称')
    def display_movie(self, obj):
        movie = obj.movie
        zh_title = getattr(movie, 'zh_title', None)
        return zh_title or movie.title

    @admin.display(description='评分', ordering='rating')
    def display_rating(self, obj):
        return f'{obj.rating} 分'

    @admin.display(description='评分时间', ordering='timestamp')
    def display_timestamp(self, obj):
        return obj.timestamp.strftime('%Y-%m-%d %H:%M')

    def get_list_display(self, request):
        return ['display_user', 'display_movie', 'display_rating', 'display_timestamp']


admin.site.site_header = '电影推荐系统后台'
admin.site.site_title = '电影推荐后台'
admin.site.index_title = '后台管理首页'