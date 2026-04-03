from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from django.db import transaction


class Command(BaseCommand):
    help = '批量将所有用户的密码设置为 user123456（加密存储，确保登录有效）'

    def add_arguments(self, parser):
        # 可选参数：指定仅更新特定前缀的用户（比如仅更新user开头的用户）
        parser.add_argument(
            '--prefix',
            type=str,
            default='user',
            help='用户用户名前缀（默认：user，匹配user1、user2等）'
        )
        # 可选参数：是否强制更新所有用户（包括非前缀匹配的）
        parser.add_argument(
            '--all',
            action='store_true',
            help='是否更新所有用户（忽略prefix参数）'
        )

    def handle(self, *args, **options):
        # 1. 筛选需要更新的用户
        prefix = options['prefix']
        if options['all']:
            users = User.objects.all()
            self.stdout.write(f'📌 开始更新【所有】用户的密码...')
        else:
            users = User.objects.filter(username__startswith=prefix)
            self.stdout.write(f'📌 开始更新前缀为「{prefix}」的用户密码...')

        if not users.exists():
            self.stdout.write(self.style.WARNING('⚠️  未找到符合条件的用户'))
            return

        # 2. 批量更新密码（事务保证原子性）
        password = 'user123456'
        encrypted_password = make_password(password)  # 加密密码
        update_count = 0

        with transaction.atomic():
            # 批量更新（按批次处理，避免内存溢出）
            batch_size = 100
            user_ids = list(users.values_list('id', flat=True))

            for i in range(0, len(user_ids), batch_size):
                batch_ids = user_ids[i:i + batch_size]
                # 批量更新密码
                update_result = User.objects.filter(id__in=batch_ids).update(
                    password=encrypted_password
                )
                update_count += update_result
                # 打印进度
                self.stdout.write(f'🔄 已更新 {min(i + batch_size, len(user_ids))}/{len(user_ids)} 个用户')

        # 3. 输出结果
        self.stdout.write(self.style.SUCCESS(
            f'✅ 密码更新完成！共更新 {update_count} 个用户，新密码：user123456'
        ))
        self.stdout.write(self.style.NOTICE(
            '📝 提示：登录时使用用户名（如user1） + 密码user123456 即可登录'
        ))