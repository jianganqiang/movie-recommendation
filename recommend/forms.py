from django import forms
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth.models import User


RATING_CHOICES = [(f'{i / 2:.1f}', f'{i / 2:.1f}') for i in range(1, 11)]


class RegisterForm(UserCreationForm):
    email = forms.EmailField(required=False, label='邮箱')

    class Meta:
        model = User
        fields = ('username', 'email', 'password1', 'password2')

    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data.get('email', '')
        if commit:
            user.save()
        return user


class LoginForm(AuthenticationForm):
    username = forms.CharField(label='用户名')
    password = forms.CharField(label='密码', widget=forms.PasswordInput)


class RatingForm(forms.Form):
    rating = forms.ChoiceField(
        choices=RATING_CHOICES,
        label='评分',
        widget=forms.RadioSelect
    )