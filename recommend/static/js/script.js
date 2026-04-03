// 为所有删除按钮添加确认提示（如果未在模板中显式添加）
document.addEventListener('DOMContentLoaded', function() {
    const deleteForms = document.querySelectorAll('form[action*="delete-rating"]');
    deleteForms.forEach(form => {
        form.addEventListener('submit', function(e) {
            if (!confirm('确定要删除这条评分吗？')) {
                e.preventDefault();
            }
        });
    });
});