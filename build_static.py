#!/usr/bin/env python3
"""
静态仪表盘构建器
================
从 cache.json 读取最新数据 → 注入 dashboard.html 模板 → 输出最终文件

用法:
  python build_static.py                        # 构建到 fifa-dashboard/dashboard.html
  python build_static.py --target ../repo/      # 构建到指定目录 (如 worldcup-pages)
  python build_static.py --collect              # 先采集数据再构建
  python build_static.py --push                 # 构建 + 自动 git commit & push
"""
import json
import os
import sys
import re
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "data" / "cache.json"
TEMPLATE_FILE = ROOT / "dashboard.html"

# ── 数据注入 ──────────────────────────────────────────────
def inject_data(template_path, cache_path, output_path):
    """将 cache.json 的数据注入到 HTML 模板的 DATA 块中"""
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    with open(cache_path, "r", encoding="utf-8") as f:
        cache = json.load(f)

    # 提取需要嵌入的数据（只取展示需要的字段）
    teams_data = []
    for t in cache.get("teams", []):
        teams_data.append({
            "rank": t["rank"], "team": t["team"], "flag": t.get("flag", ""),
            "prob": t["prob"], "trend": t.get("trend", "stable"),
            "note": t.get("note", ""),
        })

    matches_data = []
    for m in cache.get("matches", []):
        matches_data.append({
            "date": m["date"], "matchday": m.get("matchday", 0),
            "home": m["home"], "away": m["away"], "group": m.get("group", ""),
            "score": m["score"], "pred_home": m["pred_home"],
            "pred_draw": m["pred_draw"], "pred_away": m["pred_away"],
            "favorite": m.get("favorite", ""), "favorite_prob": m.get("favorite_prob", 0),
            "favorite_won": m.get("favorite_won", False),
            "volume": m.get("volume", "N/A"), "note": m.get("note", ""),
        })

    analysis_data = cache.get("analysis", {})

    # 构建新的 DATA 对象
    new_data = {
        "updated": cache.get("updated", datetime.now().strftime("%Y-%m-%d")),
        "total_volume": cache.get("total_volume", "N/A"),
        "others_prob": cache.get("others_prob", 0),
        "teams": teams_data,
        "matches": matches_data,
        "analysis": analysis_data,
    }

    new_data_json = json.dumps(new_data, ensure_ascii=False, indent=2)

    # 替换 HTML 中的 DATA 块
    # 匹配模式: const DATA = { ... };
    pattern = r"const DATA = \{[\s\S]*?\n\};"
    replacement = f"const DATA = {new_data_json};"

    new_html = re.sub(pattern, replacement, html, count=1)

    if new_html == html:
        print("❌ 未找到 DATA 块，请检查模板中是否包含 'const DATA = {...};'")
        return False

    # 更新 meta 中的日期
    today = datetime.now().strftime("%Y-%m-%d")
    new_html = re.sub(r'data-updated="[^"]*"', f'data-updated="{today}"', new_html)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(new_html)

    size_kb = len(new_html.encode("utf-8")) / 1024
    print(f"✅ 数据已注入 → {output_path} ({size_kb:.1f} KB)")
    print(f"   更新时间: {new_data['updated']}")
    print(f"   球队: {len(teams_data)} | 比赛: {len(matches_data)} | 分层: {len(analysis_data.get('tiers', []))}")
    return True


# ── Git 操作 (worldcup-pages 仓库) ─────────────────────────
def git_commit_and_push(repo_path, message=None):
    """在指定仓库中提交并推送"""
    if message is None:
        message = f"data: 数据更新 {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    try:
        cwd = os.getcwd()
        os.chdir(repo_path)

        subprocess.run(["git", "add", "dashboard.html"], check=True, capture_output=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)

        if "nothing to commit" in result.stdout + result.stderr:
            print("📭 数据无变化，跳过提交")
            return True

        subprocess.run(["git", "push", "origin", "master"], check=True)
        print(f"🚀 已推送到 GitHub → datamenu.xyz 即将更新")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Git 操作失败: {e}")
        return False
    finally:
        os.chdir(cwd)


# ── 主入口 ─────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="静态仪表盘构建器")
    parser.add_argument("--target", "-t", help="输出目录 (如 worldcup-pages 仓库路径)")
    parser.add_argument("--collect", "-c", action="store_true", help="构建前先运行 collector 采集数据")
    parser.add_argument("--push", "-p", action="store_true", help="构建后自动 git commit & push")
    parser.add_argument("--message", "-m", help="Git commit message")
    args = parser.parse_args()

    # Step 0: 可选采集
    if args.collect:
        print("📡 采集数据...")
        collector = ROOT / "collector.py"
        if collector.exists():
            subprocess.run([sys.executable, str(collector), "--once"], check=False)
        else:
            print("⚠️ collector.py 未找到，跳过采集")

    # Step 1: 注入数据
    output_path = TEMPLATE_FILE  # 默认覆盖自身
    if args.target:
        target_dir = Path(args.target).resolve()
        if not target_dir.exists():
            print(f"❌ 目标目录不存在: {target_dir}")
            return 1
        output_path = target_dir / "dashboard.html"

    if not CACHE_FILE.exists():
        print(f"❌ 数据缓存不存在: {CACHE_FILE}")
        print("   请先运行: python collector.py --once")
        return 1

    ok = inject_data(TEMPLATE_FILE, CACHE_FILE, output_path)
    if not ok:
        return 1

    # Step 2: 可选推送
    if args.push:
        repo_path = args.target or ROOT
        git_commit_and_push(repo_path, args.message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
