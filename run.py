# 影响力指数算法（权重可调）
def calculate_impact(pr):
    return (pr['comments']*0.3 + pr['reviews']*0.5 + pr['derived_commits']*1.2) * (1 + 0.1*pr['years_active'])

# 使用本地git数据替代API
git log --merges --grep "Merge pull request #" --format="%H %s" | awk '{print $5}' | cut -c2- > merged_prs.txt

