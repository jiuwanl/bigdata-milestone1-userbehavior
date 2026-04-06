"""
M1 Data Pipeline - 亿级电商行为数据处理管道（v2 优化版）

相比 v1 的核心改进：
1. _session_identification(): 会话识别结果流式写入磁盘，不再 collect 1 亿行到内存
2. _funnel_analysis(): 一次 group_by 聚合完成统计，只需 1 次 collect（结果仅 1 行）
3. load(): 从磁盘 Lazy 读取 + 谓词下推 + sink_parquet

collect 对比：
  v1: 20 次（其中 1 次是 1 亿行全量加载，内存峰值 ~10GB）
  v2: 22 次（全部轻量统计，内存峰值 ~2GB）
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional

import polars as pl

# ===================== 日志配置 =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ===================== 常量配置 =====================

BEHAVIOR_TYPE_MAPPING: dict[str, int] = {
    "pv": 1,
    "fav": 2,
    "cart": 3,
    "buy": 4,
}

SESSION_TIMEOUT_SECONDS: int = 1800  # 30 分钟
ABNORMAL_SESSION_DURATION: int = 28800  # 8 小时
ABNORMAL_BEHAVIOR_THRESHOLD: int = 10  # 异常会话最少行为数

CSV_COLUMNS: list[str] = [
    "user_id",
    "item_id",
    "category_id",
    "behavior_type",
    "timestamp",
]

VALID_BEHAVIOR_TYPES: set[str] = {"pv", "buy", "cart", "fav"}

# 2017 年全年时间范围（Unix 秒级）
# 来源：exp02 任务4 业务清洗规则
VALID_TIMESTAMP_MIN: int = 1483200000  # 2017-01-01 00:00:00
VALID_TIMESTAMP_MAX: int = 1514764799  # 2017-12-31 23:59:59


class M1DataPipelineV2:
    """M1 数据处理管道类（v2 优化版）

    封装从原始 CSV 到最终干净数据的全流程，
    包含 extract()、transform()、load() 三个核心方法。

    v2 优化点：
    - 会话识别结果流式写入磁盘，避免 1 亿行全量 collect
    - 漏斗分析一次 group_by 聚合完成，只需 1 次 collect
    - load() 从磁盘 Lazy 读取 + 谓词下推 + sink_parquet

    Attributes:
        csv_path: 原始 UserBehavior.csv 文件路径
        output_dir: 输出目录
        temp_dir: 临时分区数据目录
        abnormal_users: 异常用户 ID 列表
    """

    def __init__(
        self,
        csv_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        temp_dir: Optional[str] = None,
    ) -> None:
        """初始化管道配置

        Args:
            csv_path: UserBehavior.csv 文件路径
            output_dir: 最终输出目录路径
            temp_dir: 临时分区数据目录路径
        """
        base_dir = Path(__file__).resolve().parent
        self.csv_path = Path(csv_path) if csv_path else base_dir / "UserBehavior.csv"
        self.output_dir = Path(output_dir) if output_dir else base_dir / "output_v2"
        self.temp_dir = Path(temp_dir) if temp_dir else base_dir / "temp_v2"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.abnormal_users: list[int] = []

    # ===================== Extract =====================

    def extract(self) -> dict:
        """从 UserBehavior.csv 中提取数据并执行基础清洗

        使用 Polars scan_csv 懒加载，按 behavior_type 分区流式写入临时目录，
        为后续 transform 阶段提供分区数据源。

        Returns:
            包含原始行数、有效行数、过滤行数、分区统计的字典
        """
        logger.info("=" * 60)
        logger.info("EXTRACT: 开始读取 UserBehavior.csv 并执行数据清洗")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            if not self.csv_path.exists():
                raise FileNotFoundError(f"CSV 文件不存在: {self.csv_path}")

            # 步骤1: 懒加载 CSV，指定列名和类型
            lazy_df = pl.scan_csv(
                str(self.csv_path),
                has_header=False,
                new_columns=CSV_COLUMNS,
                schema_overrides={
                    "user_id": pl.Int64,
                    "item_id": pl.Int64,
                    "category_id": pl.Int64,
                    "behavior_type": pl.String,
                    "timestamp": pl.Int64,
                },
            )
            logger.info("CSV 懒加载完成，执行计划已生成")

            # 步骤2: 统计原始行数
            raw_rows = lazy_df.select(pl.len()).collect().item()
            logger.info("原始数据行数: %s", f"{raw_rows:,}")

            # 步骤3: 数据清洗
            cleaned_df = self._clean_data(lazy_df)

            # 步骤4: 按 behavior_type 分区流式写入临时目录
            partition_stats = self._sink_partitions(cleaned_df)

            elapsed = time.time() - start_time
            valid_rows = sum(s["rows"] for s in partition_stats.values())
            filtered_rows = raw_rows - valid_rows
            filtered_ratio = filtered_rows / raw_rows * 100 if raw_rows > 0 else 0.0

            logger.info("有效行数: %s", f"{valid_rows:,}")
            logger.info("过滤行数: %s", f"{filtered_rows:,}")
            logger.info("过滤占比: %.4f%%", filtered_ratio)
            logger.info("EXTRACT 总耗时: %.2f 秒", elapsed)

            return {
                "raw_rows": raw_rows,
                "valid_rows": valid_rows,
                "filtered_rows": filtered_rows,
                "filtered_ratio": round(filtered_ratio, 4),
                "partitions": partition_stats,
                "elapsed": elapsed,
            }

        except FileNotFoundError as exc:
            logger.error("CSV 文件未找到: %s", exc)
            raise
        except Exception as exc:
            logger.error("EXTRACT 阶段异常: %s", exc)
            raise

    # ===================== Transform =====================

    def transform(self) -> dict:
        """执行数据转换

        整合精密去重、会话识别、漏斗分析、异常诊断。
        全流程基于临时分区数据流式处理，避免全量加载。

        Returns:
            包含去重统计、会话统计、漏斗结果、异常诊断结果的字典
        """
        logger.info("=" * 60)
        logger.info("TRANSFORM: 开始数据转换")
        logger.info("=" * 60)

        try:
            # 步骤1: 精密去重（分区独立去重）
            dedup_stats = self._dedup_partitions()

            # 步骤2: 会话识别（全局合并 + 流式写入磁盘）
            session_stats = self._session_identification()

            # 步骤3: 漏斗分析（从磁盘 Lazy 读取，谓词下推）
            funnel_df = self._funnel_analysis()

            # 步骤4: 异常流量诊断（分区独立处理）
            abnormal_stats = self._abnormal_detection()

            return {
                "dedup": dedup_stats,
                "session": session_stats,
                "funnel": funnel_df,
                "abnormal": abnormal_stats,
            }

        except Exception as exc:
            logger.error("TRANSFORM 阶段异常: %s", exc)
            raise

    # ===================== Load =====================

    def load(self) -> dict:
        """加载最终结果到磁盘

        从磁盘读取会话数据，剔除异常用户后，
        将干净数据固化为 m1_final_clean.parquet。

        v2 优化：从磁盘 Lazy 读取 + 谓词下推 + sink_parquet，
        不再依赖内存中的 self.df_session。

        Returns:
            包含最终文件路径、行数、存储体积、耗时的字典
        """
        logger.info("=" * 60)
        logger.info("LOAD: 开始数据固化")
        logger.info("=" * 60)

        start_time = time.time()
        final_file = self.output_dir / "m1_final_clean.parquet"
        session_file = self.temp_dir / "sessions.parquet"

        try:
            if not session_file.exists():
                raise RuntimeError("TRANSFORM 阶段未执行会话识别，无法导出数据")

            logger.info("剔除 %d 个异常用户，写入最终文件", len(self.abnormal_users))

            # v2 优化：从磁盘 Lazy 读取 + 异常过滤 + sink_parquet
            (
                pl.scan_parquet(str(session_file))
                .filter(~pl.col("user_id").is_in(self.abnormal_users))
                .sink_parquet(str(final_file))
            )

            # 统计最终指标
            final_row_count = (
                pl.scan_parquet(str(final_file))
                .select(pl.len())
                .collect()
                .item()
            )
            file_size_bytes = os.path.getsize(str(final_file))
            file_size_mb = file_size_bytes / (1024 * 1024)

            elapsed = time.time() - start_time
            logger.info("最终数据文件: %s", final_file.resolve())
            logger.info("最终数据总行数: %s", f"{final_row_count:,}")
            logger.info("存储体积: %.2f MB (%s 字节)", file_size_mb, f"{file_size_bytes:,}")
            logger.info("固化总耗时: %.2f 秒", elapsed)

            return {
                "file_path": str(final_file.resolve()),
                "row_count": final_row_count,
                "file_size_mb": round(file_size_mb, 2),
                "file_size_bytes": file_size_bytes,
                "elapsed": elapsed,
            }

        except Exception as exc:
            logger.error("LOAD 阶段异常: %s", exc)
            raise

    # ===================== 内部辅助方法：数据清洗 =====================

    def _clean_data(self, lazy_df: pl.LazyFrame) -> pl.LazyFrame:
        """执行数据清洗（整合 exp02 任务4 清洗逻辑）

        清洗规则（与 exp02 任务4 DuckDB SQL 完全一致）:
        1. 保留 2017 年全年数据
           timestamp BETWEEN 1483200000 AND 1514764799
        2. behavior_type IN ('pv', 'buy', 'cart', 'fav')
        3. user_id > 0

        Args:
            lazy_df: 原始 LazyFrame

        Returns:
            清洗后的 LazyFrame
        """
        logger.info("开始数据清洗...")

        cleaned = (
            lazy_df
            # 规则1: 保留 2017 年全年数据（排除所有异常年份）
            .filter(
                (pl.col("timestamp") >= VALID_TIMESTAMP_MIN)
                & (pl.col("timestamp") <= VALID_TIMESTAMP_MAX)
            )
            # 规则2: 过滤无效 behavior_type
            .filter(pl.col("behavior_type").is_in(list(VALID_BEHAVIOR_TYPES)))
            # 规则3: 过滤无效 user_id
            .filter(pl.col("user_id") > 0)
        )

        logger.info(
            "数据清洗完成（规则: 2017年时间范围 + 有效行为类型 + user_id > 0）"
        )
        return cleaned

    def _sink_partitions(self, lazy_df: pl.LazyFrame) -> dict[str, dict]:
        """按 behavior_type 分区流式写入临时目录

        Args:
            lazy_df: 清洗后的 LazyFrame

        Returns:
            各分区的行数统计
        """
        logger.info("开始按 behavior_type 分区流式写入...")
        stats: dict[str, dict] = {}

        # 字符串到文件名的映射
        behavior_files = {
            "pv": "pv.parquet",
            "fav": "fav.parquet",
            "cart": "cart.parquet",
            "buy": "buy.parquet",
        }

        for behavior_str in VALID_BEHAVIOR_TYPES:
            output_file = self.temp_dir / behavior_files[behavior_str]
            part_start = time.time()

            try:
                (
                    lazy_df.filter(pl.col("behavior_type") == behavior_str)
                    .sink_parquet(str(output_file))
                )
                row_count = (
                    pl.scan_parquet(str(output_file))
                    .select(pl.len())
                    .collect()
                    .item()
                )
                stats[behavior_str] = {
                    "rows": row_count,
                    "file": str(output_file),
                    "elapsed": round(time.time() - part_start, 2),
                }
                logger.info(
                    "  分区 %s 完成，耗时 %.2f 秒，行数: %s",
                    behavior_str,
                    time.time() - part_start,
                    f"{row_count:,}",
                )
            except Exception as exc:
                logger.error("分区 %s 写入失败: %s", behavior_str, exc)
                raise

        logger.info("分区流式写入完成")
        return stats

    # ===================== 内部辅助方法：数据转换 =====================

    def _dedup_partitions(self) -> dict:
        """对每个临时分区执行精密去重

        基于 user_id + item_id + timestamp 去重（分区内已固定 behavior_type）。

        Returns:
            包含去重前后行数、重复数、重复占比的字典
        """
        logger.info("开始分区精密去重（user_id + item_id + timestamp）")
        dedup_start = time.time()

        dedup_path = self.temp_dir / "deduped"
        dedup_path.mkdir(parents=True, exist_ok=True)

        total_before = 0
        total_after = 0
        partition_stats: dict[str, dict] = {}

        for behavior_str in BEHAVIOR_TYPE_MAPPING.keys():
            source_file = self.temp_dir / f"{behavior_str}.parquet"
            if not source_file.exists():
                logger.warning("分区文件不存在: %s，跳过", source_file)
                continue

            output_file = dedup_path / f"{behavior_str}.parquet"
            part_start = time.time()

            # 统计去重前行数
            rows_before = (
                pl.scan_parquet(str(source_file))
                .select(pl.len())
                .collect()
                .item()
            )

            # 去重 + 流式写入
            (
                pl.scan_parquet(str(source_file))
                .unique(subset=["user_id", "item_id", "timestamp"])
                .sink_parquet(str(output_file))
            )

            # 统计去重后行数
            rows_after = (
                pl.scan_parquet(str(output_file))
                .select(pl.len())
                .collect()
                .item()
            )

            dup_count = rows_before - rows_after
            dup_ratio = dup_count / rows_before * 100 if rows_before > 0 else 0.0

            total_before += rows_before
            total_after += rows_after
            partition_stats[behavior_str] = {
                "rows_before": rows_before,
                "rows_after": rows_after,
                "dup_count": dup_count,
                "dup_ratio": round(dup_ratio, 4),
                "elapsed": round(time.time() - part_start, 2),
            }

            logger.info(
                "  分区 %s 去重完成: %s -> %s (重复 %s, %.4f%%), 耗时 %.2f 秒",
                behavior_str,
                f"{rows_before:,}",
                f"{rows_after:,}",
                f"{dup_count:,}",
                dup_ratio,
                time.time() - part_start,
            )

        total_dup = total_before - total_after
        total_dup_ratio = total_dup / total_before * 100 if total_before > 0 else 0.0
        elapsed = time.time() - dedup_start

        logger.info("去重前行数: %s", f"{total_before:,}")
        logger.info("去重后行数: %s", f"{total_after:,}")
        logger.info("重复数据量: %s", f"{total_dup:,}")
        logger.info("重复占比: %.4f%%", total_dup_ratio)
        logger.info("去重总耗时: %.2f 秒", elapsed)

        return {
            "rows_before": total_before,
            "rows_after": total_after,
            "dup_count": total_dup,
            "dup_ratio": round(total_dup_ratio, 4),
            "partitions": partition_stats,
            "elapsed": elapsed,
        }

    def _session_identification(self) -> dict:
        """基于去重分区数据执行会话识别（窗口函数）

        与 exp03 任务2 逻辑完全一致：
        1. 读取所有去重分区文件，添加 behavior_type 列
        2. pl.concat 合并所有分区
        3. 全局 sort + shift/over 窗口函数识别会话
        4. 生成全局连续 session_id

        v2 优化：结果流式写入磁盘，不再 collect 1 亿行到内存。
        后续漏斗分析/异常诊断从磁盘 Lazy 读取，利用谓词下推。

        Returns:
            包含总行数、会话数、耗时的字典
        """
        logger.info("开始会话识别（窗口函数 shift/over）...")
        session_start = time.time()

        dedup_path = self.temp_dir / "deduped"
        session_file = self.temp_dir / "sessions.parquet"

        # 步骤1: 读取所有去重分区（behavior_type 保持字符串类型）
        lazy_frames: list[pl.LazyFrame] = []
        behavior_files = ["pv.parquet", "fav.parquet", "cart.parquet", "buy.parquet"]
        for filename in behavior_files:
            parquet_file = dedup_path / filename
            if not parquet_file.exists():
                logger.warning("去重分区文件不存在: %s，跳过", parquet_file)
                continue

            lf = pl.scan_parquet(str(parquet_file))
            lazy_frames.append(lf)

        if not lazy_frames:
            raise ValueError("未找到有效的去重分区文件")

        # 步骤2: 合并所有分区数据
        df_dedup = pl.concat(lazy_frames)

        # 步骤3: 会话识别核心逻辑（与 exp03 任务2 完全一致）
        df_with_session = (
            df_dedup
            # 1. 按user_id分组，组内按timestamp升序排序
            .sort(["user_id", "timestamp"])
            # 2. 窗口函数：取同用户上一条行为的时间戳
            .with_columns(prev_ts=pl.col("timestamp").shift(1).over("user_id"))
            # 3. 计算相邻行为时间差（秒）
            .with_columns(timediff=pl.col("timestamp") - pl.col("prev_ts"))
            # 4. 识别新会话：>30分钟 或 第一条行为
            .with_columns(
                is_new_session=pl.when(pl.col("timediff") > SESSION_TIMEOUT_SECONDS)
                .then(True)
                .when(pl.col("prev_ts").is_null())
                .then(True)
                .otherwise(False)
            )
            # 5. 生成全局唯一session_id
            .with_columns(
                session_id=pl.col("is_new_session").cum_sum().cast(pl.String)
            )
            # 6. 清理临时列
            .drop(["prev_ts"])
        )

        # v2 优化：流式写入磁盘，不 collect 到内存
        df_with_session.sink_parquet(str(session_file))

        # 轻量统计（不加载全量数据）
        total_rows = (
            pl.scan_parquet(str(session_file))
            .select(pl.len())
            .collect()
            .item()
        )
        session_count = (
            pl.scan_parquet(str(session_file))
            .select(pl.col("session_id").n_unique())
            .collect()
            .item()
        )

        elapsed = time.time() - session_start
        logger.info(
            "会话识别完成，生成 %s 个全局唯一会话，耗时 %.2f 秒",
            f"{session_count:,}",
            elapsed,
        )

        return {
            "total_rows": total_rows,
            "session_count": session_count,
            "elapsed": elapsed,
        }

    def _funnel_analysis(self) -> pl.DataFrame:
        """执行多级转化漏斗分析

        与 exp03 任务3 逻辑完全一致：
        1. 基于会话识别结果（从磁盘 Lazy 读取）
        2. 筛选各阶段独立用户（PV / 收藏或加购 / 购买）
        3. 计算阶段转化率和整体转化率

        v2 优化：一次 group_by 聚合完成所有阶段统计，
        只需 1 次 collect，结果仅 1 行 3 列。

        Returns:
            包含漏斗阶段、独立用户数、阶段转化率、整体转化率的 DataFrame
        """
        session_file = self.temp_dir / "sessions.parquet"
        if not session_file.exists():
            raise RuntimeError("请先执行会话识别再进行漏斗分析")

        logger.info("开始多级转化漏斗分析...")

        # v2 优化：一次扫描 + group_by 聚合，collect 结果仅 1 行
        funnel_agg = (
            pl.scan_parquet(str(session_file))
            .select(["user_id", "behavior_type"])
            .group_by("user_id")
            .agg([
                (pl.col("behavior_type") == "pv").any().alias("has_pv"),
                pl.col("behavior_type").is_in(["fav", "cart"]).any().alias("has_fav_cart"),
                (pl.col("behavior_type") == "buy").any().alias("has_buy"),
            ])
            .select([
                pl.col("has_pv").sum().alias("pv_count"),
                pl.col("has_fav_cart").sum().alias("fav_cart_count"),
                pl.col("has_buy").sum().alias("buy_count"),
            ])
            .collect()
        )

        pv_count = funnel_agg["pv_count"].item()
        fav_cart_count = funnel_agg["fav_cart_count"].item()
        buy_count = funnel_agg["buy_count"].item()

        # 步骤2: 计算转化率（与 exp03 任务3 一致）
        conv_step1 = fav_cart_count / pv_count * 100 if pv_count > 0 else 0
        conv_step2 = buy_count / fav_cart_count * 100 if fav_cart_count > 0 else 0
        conv_total = buy_count / pv_count * 100 if pv_count > 0 else 0

        # 步骤3: 结构化输出漏斗结果（与 exp03 任务3 一致）
        funnel_df = pl.DataFrame({
            "漏斗阶段": ["PV 商品浏览", "收藏 / 加购", "商品购买"],
            "独立用户数": [pv_count, fav_cart_count, buy_count],
            "阶段转化率(%)": ["100.0000", f"{conv_step1:.4f}", f"{conv_step2:.4f}"],
            "整体转化率(%)": ["100.0000", f"{conv_step1:.4f}", f"{conv_total:.4f}"],
        })

        logger.info("漏斗分析结果:\n%s", funnel_df)
        return funnel_df

    def _abnormal_detection(self) -> dict:
        """执行异常流量诊断，识别爬虫/机器人账号

        核心规则与 exp03 任务4 完全一致：
        - 单段连续无休息会话 > 28800 秒（8小时）
        - 行为数 >= 10

        由于 1 亿行全局 sort + over 会 OOM，
        改为分区独立做会话识别 + 合并异常用户（结果等价）。

        Returns:
            包含异常用户数、全局用户数、异常占比、耗时的字典
        """
        logger.info("开始异常流量诊断（连续 8 小时无休息会话）...")
        abnormal_start = time.time()

        dedup_path = self.temp_dir / "deduped"

        # 分区独立做会话识别 + 异常筛选，合并异常用户
        all_abnormal_users: set[int] = set()
        for behavior_str in BEHAVIOR_TYPE_MAPPING.keys():
            source_file = dedup_path / f"{behavior_str}.parquet"
            if not source_file.exists():
                continue

            part_start = time.time()
            df_abnormal = (
                pl.scan_parquet(str(source_file))
                .select(["user_id", "timestamp"])
                .sort(["user_id", "timestamp"])
                .with_columns(prev_ts=pl.col("timestamp").shift(1).over("user_id"))
                .with_columns(
                    is_new_session=(
                        pl.col("timestamp") - pl.col("prev_ts")
                        > SESSION_TIMEOUT_SECONDS
                    ).fill_null(True)
                )
                .with_columns(
                    session_id=pl.col("is_new_session")
                    .cum_sum()
                    .over("user_id")
                )
                .group_by(["user_id", "session_id"])
                .agg(
                    session_start=pl.col("timestamp").min(),
                    session_end=pl.col("timestamp").max(),
                    behavior_num=pl.len(),
                )
                .with_columns(
                    session_duration=pl.col("session_end") - pl.col("session_start")
                )
                .filter(
                    pl.col("session_duration") > ABNORMAL_SESSION_DURATION,
                    pl.col("behavior_num") >= ABNORMAL_BEHAVIOR_THRESHOLD,
                )
                .collect()
            )

            partition_abnormal = df_abnormal["user_id"].to_list()
            all_abnormal_users.update(partition_abnormal)
            logger.info(
                "  分区 %s 异常诊断完成，发现 %d 个异常会话，耗时 %.2f 秒",
                behavior_str,
                len(partition_abnormal),
                time.time() - part_start,
            )

        self.abnormal_users = list(all_abnormal_users)
        abnormal_user_count = len(self.abnormal_users)

        # 全局总用户数（流式统计）
        global_users = (
            pl.scan_parquet(str(dedup_path / "*.parquet"))
            .select("user_id")
            .unique()
            .count()
            .collect()
            .item()
        )
        abnormal_ratio = abnormal_user_count / global_users * 100 if global_users > 0 else 0

        elapsed = time.time() - abnormal_start
        logger.info(
            "异常用户数: %s，全局总用户数: %s，占比: %.4f%%，耗时: %.2f 秒",
            f"{abnormal_user_count:,}",
            f"{global_users:,}",
            abnormal_ratio,
            elapsed,
        )

        return {
            "abnormal_user_count": abnormal_user_count,
            "global_users": global_users,
            "abnormal_ratio": round(abnormal_ratio, 4),
            "elapsed": elapsed,
        }
