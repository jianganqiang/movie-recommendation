from decimal import Decimal
from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator


class Movie(models.Model):
    id = models.IntegerField(primary_key=True)  # 使用 MovieLens 的 movieId
    title = models.CharField(max_length=255)
    zh_title = models.CharField(max_length=255, blank=True, null=True)  # 新增：中文片名

    genres = models.CharField(max_length=255)
    zh_genres = models.CharField(max_length=255, blank=True, null=True)  # 新增：中文类型
    poster = models.CharField(max_length=500, blank=True, null=True)  # 图片路径或URL
    overview = models.TextField(blank=True, null=True)  # 新增：电影简介
    avg_rating = models.FloatField(default=0.0)
    rating_count = models.IntegerField(default=0)

    def __str__(self):
        return self.title


class Rating(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    rating = models.DecimalField(
        max_digits=2,
        decimal_places=1,
        validators=[
            MinValueValidator(Decimal('0.5')),
            MaxValueValidator(Decimal('5.0')),
        ]
    )
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'movie')  # 防止重复评分

    def __str__(self):
        return f'{self.user.username} - {self.movie.title} - {self.rating}'