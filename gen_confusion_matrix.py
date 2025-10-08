"""
Command-line tool to build a per-term confusion matrix from radiology report
comparisons using pre-labeled FN/FP/TN/TP columns.

This version parses the fn_Positive, fp_Positive, tn_Positive, tp_Positive columns
to compute aggregate counts and ensures accurate metric calculations.

Usage:
    python build_confusion_matrix_modified.py <file_path>

Notes:
- Input Excel must include "fn_Positive", "fp_Positive", "tn_Positive", "tp_Positive" columns with newline-separated terms.
- Output Excel is saved as <input_stem>_confusion_matrix.xlsx with sheets: "Confusion Matrix", "Positive".
- Metrics are calculated as specified.
"""

import pandas as pd
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Sequence
from pathlib import Path
import logging
import argparse

# ---------------------------------------------------------------------------
# Configuration & constants
# ---------------------------------------------------------------------------

TERMS: List[str] = [
    "perihilar_infiltrate",
    "pneumonia",
    "bronchitis",
    "interstitial",
    "diseased_lungs",
    "hypo_plastic_trachea",
    "cardiomegaly",
    "pulmonary_nodules",
    "pleural_effusion",
    "rtm",
    "focal_caudodorsal_lung",
    "focal_perihilar",
    "pulmonary_hypoinflation",
    "right_sided_cardiomegaly",
    "pericardial_effusion",
    "bronchiectasis",
    "pulmonary_vessel_enlargement",
    "left_sided_cardiomegaly",
    "thoracic_lymphadenopathy",
    "esophagitis",
    "vhs_v2",
]

REQUIRED_OUTPUT_COLS: List[str] = [
    "condition",
    "tp_Positive",
    "fn_Positive",
    "tn_Positive",
    "fp_Positive",
    "Sensitivity",
    "Specificity",
    "Check",
    "Positive Ground Truth",
    "Negative Ground Truth",
    "Ground Truth Check",
    "",
    "Radiologist Agreement Rate",
]

FN_COL = "fn_Positive"
FP_COL = "fp_Positive"
TN_COL = "tn_Positive"
TP_COL = "tp_Positive"

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class Counts:
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0


# ---------------------------------------------------------------------------
# Parsing utilities
# ---------------------------------------------------------------------------


def parse_term_list(text: Optional[str]) -> set[str]:
    """Parse newline or space-separated terms into a set."""
    if not isinstance(text, str) or not text.strip():
        return set()
    terms = re.split(r"\n|\s+", text.strip())
    return {t.strip() for t in terms if t.strip()}


# ---------------------------------------------------------------------------
# Confusion matrix computation
# ---------------------------------------------------------------------------


def compute_confusion_matrix(
    df: pd.DataFrame,
    terms: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute per-term confusion matrix using pre-labeled columns."""
    counts: Dict[str, Counts] = {t: Counts() for t in terms}
    detail_rows: List[Dict[str, object]] = []
    total_rows = len(df)

    for idx, row in df.iterrows():
        fn_set = parse_term_list(row.get(FN_COL))
        fp_set = parse_term_list(row.get(FP_COL))
        tn_set = parse_term_list(row.get(TN_COL))
        tp_set = parse_term_list(row.get(TP_COL))

        detail = {"RowIndex": idx}
        for term in terms:
            if term in tp_set:
                counts[term].tp += 1
                ai_pos = 1
                gt_pos = 1
            elif term in fp_set:
                counts[term].fp += 1
                ai_pos = 1
                gt_pos = 0
            elif term in fn_set:
                counts[term].fn += 1
                ai_pos = 0
                gt_pos = 1
            elif term in tn_set:
                counts[term].tn += 1
                ai_pos = 0
                gt_pos = 0
            else:
                # Assume TN if not listed
                counts[term].tn += 1
                ai_pos = 0
                gt_pos = 0
            detail[f"AI_{term}"] = ai_pos
            detail[f"GT_{term}"] = gt_pos
        detail_rows.append(detail)

    records: List[Dict[str, object]] = []
    for term in terms:
        c = counts[term]
        support = c.tp + c.fn
        predicted_pos = c.tp + c.fp
        total = c.tp + c.fp + c.fn + c.tn

        records.append(
            {
                "Term": term,
                "TP": c.tp,
                "FP": c.fp,
                "FN": c.fn,
                "TN": c.tn,
                "Support": support,
                "Predicted_Positive": predicted_pos,
            }
        )

    confusion_df = pd.DataFrame.from_records(records)
    per_row_df = pd.DataFrame.from_records(detail_rows)
    return confusion_df, per_row_df


def build_requested_output(confusion_df: pd.DataFrame) -> pd.DataFrame:
    """Transform to requested schema with specified formulas."""
    df = confusion_df.copy()
    df["Positive Ground Truth"] = df["TP"] + df["FN"]
    df["Negative Ground Truth"] = df["TN"] + df["FP"]
    df["Check"] = df["TP"] + df["FP"] + df["FN"] + df["TN"]
    df["Sensitivity"] = df.apply(
        lambda r: r["TP"] / (r["TP"] + r["FN"]) if (r["TP"] + r["FN"]) != 0 else 0.0,
        axis=1,
    )
    df["Specificity"] = df.apply(
        lambda r: r["TN"] / (r["TN"] + r["FP"]) if (r["TN"] + r["FP"]) != 0 else 0.0,
        axis=1,
    )
    df["Radiologist Agreement Rate"] = df.apply(
        lambda r: (r["TP"] + r["TN"]) / r["Check"] if r["Check"] != 0 else 0.0, axis=1
    )
    df["Ground Truth Check"] = df["Positive Ground Truth"] + df["Negative Ground Truth"]

    out = pd.DataFrame(
        {
            "condition": df["Term"],
            "tp_Positive": df["TP"],
            "fn_Positive": df["FN"],
            "tn_Positive": df["TN"],
            "fp_Positive": df["FP"],
            "Sensitivity": df["Sensitivity"].round(4),
            "Specificity": df["Specificity"].round(4),
            "Check": df["Check"],
            "Positive Ground Truth": df["Positive Ground Truth"],
            "Negative Ground Truth": df["Negative Ground Truth"],
            "Ground Truth Check": df["Ground Truth Check"],
            "Radiologist Agreement Rate": df["Radiologist Agreement Rate"].round(4),
        }
    )
    out.insert(11, "", "")
    out = out[REQUIRED_OUTPUT_COLS]
    return out


# ---------------------------------------------------------------------------
# Build details sheet
# ---------------------------------------------------------------------------


def build_details_df(
    input_df: pd.DataFrame, per_row_df: pd.DataFrame, terms: Sequence[str]
) -> pd.DataFrame:
    """Build details for 'Positive' sheet."""
    details_df = input_df.copy().reset_index(drop=True)
    details_df["Original Radiologist"] = ""

    fn_lists = []
    fp_lists = []
    tp_lists = []
    tn_lists = []
    for i in range(len(per_row_df)):
        fn = []
        fp = []
        tp = []
        tn = []
        for term in terms:
            ai = per_row_df[f"AI_{term}"][i]
            gt = per_row_df[f"GT_{term}"][i]
            ai_val = "Positive" if ai else "Negative"
            gt_val = "Positive" if gt else "Negative"
            val_str = f"{term} - (AI: {ai_val}, Original: {gt_val})"
            if ai == 0 and gt == 1:
                fn.append(val_str)
            elif ai == 1 and gt == 0:
                fp.append(val_str)
            elif ai == 1 and gt == 1:
                tp.append(val_str)
            elif ai == 0 and gt == 0:
                tn.append(val_str)
        fn_lists.append(" ".join(fn))
        fp_lists.append(" ".join(fp))
        tp_lists.append(" ".join(tp))
        tn_lists.append(" ".join(tn))

    details_df["False Negative"] = fn_lists
    details_df["False Positive"] = fp_lists
    details_df["True Positive"] = tp_lists
    details_df["True Negative"] = tn_lists

    for term in terms:
        details_df[f"{term}_AI"] = [
            "Positive" if per_row_df[f"AI_{term}"][i] else "Negative"
            for i in range(len(per_row_df))
        ]

    details_df["gpt_charge"] = 0.0

    for term in terms:
        categories = []
        for i in range(len(per_row_df)):
            ai = per_row_df[f"AI_{term}"][i]
            gt = per_row_df[f"GT_{term}"][i]
            if ai == 1 and gt == 1:
                categories.append("True Positive")
            elif ai == 1 and gt == 0:
                categories.append("False Positive")
            elif ai == 0 and gt == 1:
                categories.append("False Negative")
            else:
                categories.append("True Negative")
        details_df[term] = categories

    details_df[""] = ""
    details_df[" "] = ""

    for term in terms:
        details_df[f"{term}_Original"] = [
            "Positive" if per_row_df[f"GT_{term}"][i] else "Negative"
            for i in range(len(per_row_df))
        ]

    return details_df


# ---------------------------------------------------------------------------
# I/O and CLI
# ---------------------------------------------------------------------------


def process_file(
    file_path: Path,
    terms: Sequence[str],
) -> Path:
    """Process input and write output."""
    logging.info("Reading input Excel: %s", file_path)
    df = pd.read_excel(file_path, engine="openpyxl")
    confusion_df, per_row_df = compute_confusion_matrix(df, terms)
    requested_df = build_requested_output(confusion_df)
    details_df = build_details_df(df, per_row_df, terms)
    out_path = file_path.with_name(f"{file_path.stem}_confusion_matrix.xlsx")
    logging.info("Writing output Excel: %s", out_path)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        requested_df.to_excel(writer, index=False, sheet_name="Confusion Matrix")
        details_df.to_excel(writer, index=False, sheet_name="Positive")
    return out_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build confusion matrix from labeled Excel."
    )
    parser.add_argument(
        "file_path",
        type=str,
        help="Path to input Excel (.xlsx) file",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = build_arg_parser()
    args = parser.parse_args()

    file_path = Path(args.file_path).expanduser().resolve()

    try:
        out_path = process_file(
            file_path=file_path,
            terms=TERMS,
        )
        print(str(out_path))
    except Exception as exc:
        logging.error("Processing failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
