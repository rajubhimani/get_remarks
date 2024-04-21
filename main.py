"""main module"""

import warnings
import logging
import json
from pathlib import PurePath
from io import StringIO, BytesIO
from zipfile import ZipFile, ZIP_DEFLATED, ZipInfo
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import Response
import pandas as pd
warnings.filterwarnings("ignore")
app = FastAPI()
# get root logger
logger = logging.getLogger(__name__)

@app.get("/")
async def root() -> dict:
    """root

    Returns:
        _type_: _description_
    """
    return {"message": "Hello World"}


@app.post("/get_gst_remarks/", response_description='zip')
async def get_gst_remarks(
    gov_file: UploadFile,
    client_file: UploadFile,
    config_file: UploadFile,
):
    """get_gst_remarks

    Args:
        gov_file (UploadFile): Government Tax CSV File
        client_file (UploadFile): Client Tax CSV SFile
        config_file (UploadFile): Configuration JSON File

    Returns:
        _type_: _description_
    """
    config_dict = json.load(config_file.file)
    output_path = config_dict["output_path"]
    output_prefix = config_dict["output_prefix"]
    gov_data = pd.read_csv(gov_file.file, dtype=str)
    user_data = pd.read_csv(client_file.file, dtype=str)
    logger.debug(gov_data.columns)
    logger.debug(user_data.columns)
    output_path = PurePath(output_path)

    def remove_comma(value: str):
        """_summary_

        Args:
            value (str): _description_

        Returns:
            _type_: _description_
        """
        if pd.isna(value):
            return value
        return value.replace(",", "")

    tax_columns = ["igst", "cgst", "sgst"]
    match_columns = ["gstin", "invoice_no"]
    all_columns = []
    left_join_column = []
    right_join_column = []
    left_value_column = []
    right_value_column = []

    for column in match_columns:
        gov_column = config_dict["gov_columns"][column]
        ren_col = "gov_" + column
        gov_data[ren_col] = gov_data[gov_column]
        gov_data.drop(columns=[gov_column])
        left_join_column.append(ren_col)
        all_columns.append(ren_col)
        user_column = config_dict["user_columns"][column]
        ren_col = "user_" + column
        user_data[ren_col] = user_data[user_column]
        user_data.drop(columns=[user_column])
        right_join_column.append(ren_col)
        all_columns.append(ren_col)
    for column in tax_columns:
        gov_column = config_dict["gov_columns"][column]
        ren_col = "gov_" + column
        gov_data[ren_col] = gov_data[gov_column]
        gov_data.drop(columns=[gov_column])
        gov_data[ren_col] = gov_data[ren_col].map(remove_comma)
        gov_data[ren_col] = gov_data[ren_col].astype(float)
        all_columns.append(ren_col)
        left_value_column.append(ren_col)
        user_column = config_dict["user_columns"][column]
        ren_col = "user_" + column
        user_data[ren_col] = user_data[user_column]
        user_data.drop(columns=[user_column])
        user_data[ren_col] = user_data[ren_col].map(remove_comma)
        user_data[ren_col] = user_data[ren_col].astype(float)
        all_columns.append(ren_col)
        right_value_column.append(ren_col)

    def apply_remarks(row):
        """_summary_

        Args:
            row (_type_): _description_
            remarks (_type_): _description_

        Returns:
            _type_: _description_
        """
        remark_list = []
        remarks = [
            (
                "igst",
                "less than",
                "Excess IGST Credit taken",
            ),
            (
                "igst",
                "greater than",
                "Less IGST Credit taken",
            ),
            (
                "cgst",
                "less than",
                "Excess CGST Credit taken",
            ),
            (
                "cgst",
                "greater tha",
                "Less CGST Credit taken",
            ),
            (
                "sgst",
                "less than",
                "Excess SGST Credit taken",
            ),
            (
                "sgst",
                "greater than",
                "Less SGST Credit taken",
            ),
        ]

        for remark_info in remarks:
            col, remark_cond, remark = remark_info
            gov_col = "gov_" + col
            user_col = "user_" + col
            if remark_cond == "less than":
                if row[gov_col] < row[user_col]:
                    remark_list.append(remark)
            elif remark_cond == "greater than":
                if row[gov_col] > row[user_col]:
                    remark_list.append(remark)
            elif remark_cond == "equals":
                if row[gov_col] == row[user_col]:
                    remark_list.append(remark)
            elif remark_cond == "not equals":
                if row[gov_col] == row[user_col]:
                    remark_list.append(remark)
        if not remark_list:
            remark_list.append("all good")
        return ", ".join(remark_list)

    logger.debug(
        "left_join_column - %s, right_join_column - %s",
        left_join_column,
        right_join_column,
    )
    comb_data = pd.merge(
        gov_data,
        user_data,
        # on=match_columns,
        left_on=left_join_column,
        right_on=right_join_column,
        how="outer",
        indicator=True,
    )
    logger.debug(
        "comb_data.columns - %s, comb_data.shape %s", comb_data.columns, comb_data.shape
    )
    
    zip_buffer = BytesIO()
    try:
        with ZipFile(zip_buffer, 'w', ZIP_DEFLATED) as zip_file:
            match_filter = comb_data["_merge"] == "both"
            match_data = comb_data.loc[match_filter, :]
            logger.debug("%s %s", match_data.columns, match_data.shape)

            match_data["remarks"] = match_data.apply(apply_remarks, axis="columns")
            logger.debug(
                "match_data.columns - %s,  match_data.shape- %s",
                match_data.columns,
                match_data.shape,
            )
            logger.debug("all_columns - %s", all_columns)
            gst_zip_info = ZipInfo(output_prefix + "_gst.csv")
            gst_s_io = StringIO()
            match_data[all_columns + ["remarks"]].to_csv(gst_s_io, index=False)
            zip_file.writestr(gst_zip_info, gst_s_io.getvalue())
            remain_filter = comb_data["_merge"] != "both"
            remain_data = comb_data.loc[remain_filter, :]
            remain_gov_data = remain_data.loc[:, left_join_column + left_value_column]
            remain_gov_data = remain_gov_data.dropna(how="all", axis="index")
            remain_user_data = remain_data.loc[:, right_join_column + right_value_column]
            remain_user_data = remain_user_data.dropna(how="all", axis="index")
            gstin_data = pd.merge(
                remain_gov_data,
                remain_user_data,
                # on=match_columns,
                left_on=left_join_column[0],
                right_on=right_join_column[0],
                how="outer",
                indicator=True,
            )
            gstin_match_filter = gstin_data["_merge"] == "both"
            gstin_not_match_filter = gstin_data["_merge"] != "both"
            gstin_match_data = gstin_data.loc[gstin_match_filter, :]
            gstin_match_data["remarks"] = "invoice no don't match"
            gstin_not_match_data = gstin_data.loc[gstin_not_match_filter, :]
            gstin_zip_info = ZipInfo(output_prefix + "_gstin.csv")
            gstin_s_io = StringIO()
            gstin_match_data[all_columns + ["remarks"]].to_csv(gstin_s_io, index=False)
            zip_file.writestr(gstin_zip_info, gstin_s_io.getvalue())

            invoice_gov_data = gstin_not_match_data.loc[:, left_join_column + left_value_column]
            invoice_gov_data = invoice_gov_data.dropna(how="all", axis="index")
            invoice_user_data = gstin_not_match_data.loc[
                :, right_join_column + right_value_column
            ]
            invoice_user_data = invoice_user_data.dropna(how="all", axis="index")
            invoice_data = pd.merge(
                invoice_gov_data,
                invoice_user_data,
                left_on=left_join_column[1],
                right_on=right_join_column[1],
                how="outer",
                indicator=True,
            )
            invoice_match_filter = invoice_data["_merge"] == "both"
            invoice_not_match_filter = invoice_data["_merge"] != "both"
            invoice_data.loc[invoice_match_filter, "remarks"] = "gstin not matchec"
            invoice_data.loc[invoice_not_match_filter, "remarks"] = "no match record"
            invoice_zip_info = ZipInfo(output_prefix + "_invoice.csv")
            invoice_s_io = StringIO()
            invoice_data[all_columns + ["remarks"]].to_csv(invoice_s_io, index=False)
            zip_file.writestr(invoice_zip_info, invoice_s_io.getvalue())
        zip_buffer.seek(0)
        headers: dict[str, str] = {"Content-Disposition": "attachment; filename=gst_remarks.zip"}
        return Response(
            zip_buffer.getvalue(),
            status_code=200,
            headers=headers,
            media_type="application/zip")
    except Exception as error:
        logger.error(error)
        raise HTTPException(detail='There was an error processing the data', status_code=400) from error
    finally:
        zip_buffer.close()
