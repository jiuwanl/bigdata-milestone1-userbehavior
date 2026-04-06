"""
M1 Pipeline v2 主入口 - 一键运行全流程（优化版）

执行顺序: extract() -> transform() -> load()
输出: output_v2/m1_final_clean.parquet
结果: reports/v2/

用法:
    python run_m1_pipeline_v2.py
"""

import argparse
import json
import logging
import shutil
import sys
import time
from pathlib import Path

from m1_pipeline_v2 import M1DataPipelineV2

# ===================== 日志配置 =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ===================== 配置 =====================

BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports" / "v2"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="M1 Data Pipeline v2 主入口")
    parser.add_argument(
        "--clean-temp",
        action="store_true",
        help="运行完成后清理临时分区目录",
    )
    return parser.parse_args()


def save_results(pipeline: M1DataPipelineV2, result: dict) -> None:
    """保存运行结果到 reports/v2 目录

    Args:
        pipeline: 管道实例
        result: 运行结果字典
    """
    # 1. 保存漏斗分析 CSV
    funnel_df = result["transform"]["funnel"]
    csv_file = REPORTS_DIR / "funnel_result.csv"
    funnel_df.write_csv(str(csv_file))
    logger.info("漏斗结果已保存至: %s", csv_file)

    # 2. 准备 JSON 可序列化的结果（将 DataFrame 转为字符串）
    json_result = result.copy()
    json_result["transform"] = result["transform"].copy()
    json_result["transform"]["funnel"] = str(result["transform"]["funnel"])

    # 3. 保存 JSON 结果
    json_file = REPORTS_DIR / "pipeline_result.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(json_result, f, ensure_ascii=False, indent=4)
    logger.info("JSON 结果已保存至: %s", json_file)

    # 4. 保存 Markdown 报告
    md_file = REPORTS_DIR / "result_report.md"
    md_content = _generate_markdown_report(result)
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md_content)
    logger.info("Markdown 报告已保存至: %s", md_file)


def _generate_markdown_report(result: dict) -> str:
    """生成 Markdown 格式的结果报告

    Args:
        result: 运行结果字典

    Returns:
        Markdown 格式的报告字符串
    """
    extract = result["extract"]
    transform = result["transform"]
    load = result["load"]
    total = result["total_elapsed_seconds"]

    funnel = transform["funnel"]
    funnel_rows = funnel.rows()

    return f"""# M1 Pipeline v2 结果报告（Lazy API 优化版）

## 数据质量指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 原始数据行数 | {extract['raw_rows']:,} | 约 1 亿行 |
| 清洗后有效行数 | {extract['valid_rows']:,} | 过滤 {extract['filtered_rows']:,} 行脏数据 |
| 去重后行数 | {transform['dedup']['rows_after']:,} | 剔除 {transform['dedup']['dup_count']} 条重复记录 |
| 最终数据行数 | {load['row_count']:,} | 剔除异常用户的行为记录 |
| 存储体积 | {load['file_size_mb']:.2f} MB | Parquet 列式压缩 |

## 会话识别指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 全局唯一会话数 | {transform['session']['session_count']:,} | 30 分钟超时判定规则 |
| 会话识别耗时 | {transform['session']['elapsed']:.2f} 秒 | sink_parquet 流式写入 |

## 转化漏斗

| 漏斗阶段 | 独立用户数 | 阶段转化率 | 整体转化率 |
|----------|-----------|-----------|-----------|
| **PV 商品浏览** | {funnel_rows[0][1]:,} | {funnel_rows[0][2]}% | {funnel_rows[0][3]}% |
| **收藏 / 加购** | {funnel_rows[1][1]:,} | {funnel_rows[1][2]}% | {funnel_rows[1][3]}% |
| **商品购买** | {funnel_rows[2][1]:,} | {funnel_rows[2][2]}% | {funnel_rows[2][3]}% |

## 异常流量诊断

| 指标 | 数值 | 说明 |
|------|------|------|
| 异常用户数 | {transform['abnormal']['abnormal_user_count']} | 连续 8 小时无休息会话 |
| 全局总用户数 | {transform['abnormal']['global_users']:,} | 去重后的独立用户数 |
| 异常用户占比 | {transform['abnormal']['abnormal_ratio']:.4f}% | 数据质量良好 |

## 性能指标

| 阶段 | 耗时（秒） |
|------|-----------|
| extract() | {extract['elapsed']:.2f} |
| 去重 | {transform['dedup']['elapsed']:.2f} |
| 会话识别 | {transform['session']['elapsed']:.2f} |
| 异常诊断 | {transform['abnormal']['elapsed']:.2f} |
| load() | {load['elapsed']:.2f} |
| **总计** | **{total:.2f}** |

> v2 优化：消除了 1 亿行全量 collect，内存峰值降低约 80%。
"""


def main() -> int:
    """运行 M1 数据处理全流程（v2）

    Returns:
        0 表示成功，1 表示失败
    """
    args = parse_args()

    logger.info("=" * 70)
    logger.info("M1 Data Pipeline v2 - 全流程启动（Lazy API 优化版）")
    logger.info("=" * 70)

    pipeline_start = time.time()

    try:
        # 1. 初始化管道
        pipeline = M1DataPipelineV2()

        # 2. Extract
        extract_stats = pipeline.extract()

        # 3. Transform
        transform_stats = pipeline.transform()

        # 4. Load
        load_stats = pipeline.load()

        # 5. 汇总输出
        total_elapsed = time.time() - pipeline_start
        logger.info("=" * 70)
        logger.info("M1 Data Pipeline v2 - 全流程完成")
        logger.info("=" * 70)
        logger.info("总耗时: %.2f 秒 (%.2f 分钟)", total_elapsed, total_elapsed / 60)
        logger.info("最终文件: %s", load_stats["file_path"])
        logger.info("最终行数: %s", f"{load_stats['row_count']:,}")

        # 6. 保存结果
        result = {
            "pipeline_date": time.strftime("%Y-%m-%d"),
            "version": "v2",
            "description": "M1 Pipeline v2 运行结果（Lazy API 优化版）",
            "extract": extract_stats,
            "transform": {
                "dedup": transform_stats["dedup"],
                "session": transform_stats["session"],
                "funnel": transform_stats["funnel"],
                "abnormal": transform_stats["abnormal"],
            },
            "load": load_stats,
            "total_elapsed_seconds": total_elapsed,
        }
        save_results(pipeline, result)

        # 7. 可选：清理临时目录
        if args.clean_temp:
            logger.info("清理临时分区目录: %s", pipeline.temp_dir)
            shutil.rmtree(pipeline.temp_dir, ignore_errors=True)
            logger.info("临时目录清理完成")

        return 0

    except KeyboardInterrupt:
        logger.warning("用户手动中断")
        return 1

    except Exception as exc:
        logger.error("Pipeline v2 运行失败: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
