from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine
import numpy as np
import pandas as pd

app = FastAPI()


DATABASE_URL = "postgresql://postgres:password@localhost:5432/database_name"
TABLE_NAME = "employee_data"  

engine = create_engine(DATABASE_URL)


@app.get("/")
def root():
    return {"message": "Dataset manipulation with FastApi !!"}


@app.get("/get-dataset")
def get_dataset(rows: int = 100):

    try:
        query = f"SELECT * FROM {TABLE_NAME} LIMIT {rows}"
        df = pd.read_sql_query(query, engine)

        data = df.to_dict(orient="records")
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/describe-dataset")
def describe_dataset():

    try:
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql_query(query, engine)

        description = df.describe(include="all")

        description_dict = description.to_dict()

        json_compliant_description = {
            key: {k: (None if pd.isna(v) or pd.isnull(v) else v)
                  for k, v in value.items()}
            for key, value in description_dict.items()
        }

        return JSONResponse(content=json_compliant_description)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/handle-nulls")
def handle_nulls(replace_with: int = 0):

    try:
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql_query(query, engine)

        if df.isnull().sum().sum() == 0:
            return {"message": "No null values found in the dataset."}

        df.fillna(replace_with, inplace=True)
        return {"message": "Null values replaced", "replaced_with": replace_with}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/preprocess-data")
def preprocess_data():

    try:

        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql_query(query, engine)

        df.drop_duplicates(inplace=True)

        num_cols = df.select_dtypes(include=["int", "float"]).columns
        if len(num_cols) > 0:
            col = num_cols[0]
            df[f"normalized_{col}"] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

        return {"message": "Data preprocessing complete. Duplicates removed and first numerical column normalized."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/create-features")
def create_features():

    try:
        query = "SELECT * FROM employee_data"
        df = pd.read_sql_query(query, engine)

        num_cols = df.select_dtypes(include=["int", "float"]).columns
        if len(num_cols) > 0:
            col = num_cols[0]

            df[f"log_{col}"] = df[col].apply(lambda x: np.log(x) if x > 0 else 0)

            df["category"] = df[col].apply(lambda x: "High" if x > 1000 else "Low")

        return {"message": "Feature creation complete. New features added."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
