import pickle
from django.core.management.base import BaseCommand
from recommend.utils import compute_similarity, SIM_PATH

class Command(BaseCommand):
    help = '计算物品相似度并保存'

    def handle(self, *args, **options):
        sim_dict = compute_similarity()
        with open(SIM_PATH, 'wb') as f:
            pickle.dump(sim_dict, f)
        self.stdout.write('相似度计算完成并保存')
