import pandas as pd
import requests
import camelot
import numpy as np
import warnings
from bs4 import BeautifulSoup
import tempfile

def transform_year(year):
    """
    Transform the date as 2022 to 2022/2023
    """
    year = int(year)
    return f"{year}/{year+1}"

def separate(x):
    if len(x) < 2:
        x.insert(0, np.nan)
    elif len(x) > 2:
        x = [element for element in x if element.strip() != ""]
    return x

def separate_cols(DF):
    colnames = []
    for col in DF.columns:
        for element in DF[col].astype(str).values:
            if "Fixed Variable" in element:
                DF = DF.replace(
                    {col: {"Fixed Variable": "Fixed\nVariable"}}, regex=True
                )
                element = "Fixed\nVariable"
            if "Fixed\nVariable" in element:
                colnames.append(col)
                split_col = DF[col].str.split("\n")
                split_col = split_col.apply(lambda x: separate(x))
                DF[[str(col) + "_Var", str(col) + "_Fix"]] = pd.DataFrame(
                    split_col.to_list(), index=DF.index
                )
                break

    DF = DF.drop(colnames, axis=1)
    return DF

def get_info(DF):
    names = [
        "soybeans following corn",
        "drilled soybeans following corn",
        "corn following soybeans",
    ]
    Product = []
    Yield = []

    for i in range(0, 6):
        infos = DF.iloc[i].replace("", np.nan).dropna().unique()
        for info in infos:
            if info == "Variable" or info == "Fixed":
                max_index = i
            for name in names:
                if isinstance(info, str) and info.lower().strip() == name:
                    Product.append(info)
        for info in infos:
            try:
                if float(info.split(" ")[0]):
                    Yield.append(info.split(" ")[0])
            except:
                pass

    return Product, Yield, max_index

def columns_selection(DF):
    value_to_keep = ["Fixed", "Variable"]
    columns_to_keep = DF.columns[DF.isin(value_to_keep).any()]

    # Créer un nouveau DataFrame avec les colonnes filtrées
    selected_columns = [0] + list(columns_to_keep)
    DF = DF[selected_columns]
    return DF

def convert_to_float(value):
    try:
        return float(value)
    except ValueError:
        return None

def final_transformation(DF):
    Rotation = []
    Tilled = []
    for product in DF["Commodity"]:
        if "corn following corn" in product.lower():
            Rotation.append(np.nan)
        elif "following" in product.lower():
            Rotation.append("Rotation")
        else:
            Rotation.append(np.nan)
        if "till" in product.lower() and "no" in product.lower():
            Tilled.append(np.nan)
        elif "till" in product.lower() and "low-till" in product.lower():
            Tilled.append("Low Till")
        elif "till" in product.lower() and "strip tillage" in product.lower():
            Tilled.append("Strip Till")
        else:
            Tilled.append(np.nan)
    DF["Rotation"] = Rotation
    DF["Tilled"] = Tilled
    DF = DF.drop("Type", axis=1)
    Value = []
    for value in DF["Values"]:
        Value.append(value.split(" ")[0])
    DF["Value"] = Value
    DF["Value"] = DF["Value"].apply(convert_to_float)
    DF = DF.dropna(subset=["Value"])
    DF = DF.drop("Values", axis=1)
    return DF

url = 'https://www.extension.iastate.edu/agdm/crops/html/a1-20.html'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
tags = soup.find_all('p')

for tag in tags:
    if "Estimated" in tag.text:
        link = tag.find('a')['href']

pdf_link = url.split('html')[0] + link.split('../')[-1]
print(pdf_link)

def extract_data(url):
    DATA = pd.DataFrame()
    year = int(url[-8:-4])
    product = []
    response = requests.get(url)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        temp_file.write(response.content)
        for page in range(2, 6):
            Title = camelot.read_pdf(
                temp_file.name,
                pages=str(page),
                flavor="stream",
                row_tol=10,
                table_areas=["10,700,490,660"],
            )

            extract_area = ["50,650,600,90"]
            DF = Title[0].df
            focus = [
                "corn following corn",
                "corn following soybeans",
                "corn silage following corn",
                "herbicide tolerant soybeans following corn",
            ]
            for index, row in DF.iterrows():
                for column in DF.columns:
                    cell_value = row[column]
                    for name in focus:
                        if (
                            isinstance(cell_value, str)
                            and name == cell_value.lower().strip()
                        ):
                            product.append([page, name])
                            Extract = camelot.read_pdf(
                                temp_file.name,
                                pages=str(page),
                                flavor="stream",
                                row_tol=0.1,
                                table_areas=extract_area,
                            )
                            DF = Extract[0].df
                            DF = separate_cols(DF)
                            Yield = get_info(DF)[1]
                            DF = DF.iloc[get_info(DF)[2] :]
                            DF = columns_selection(DF)
                            DF1, DF2, DF3 = (
                                pd.DataFrame(
                                    DF.iloc[
                                        :, [0, DF.shape[1] - 6, DF.shape[1] - 5]
                                    ]
                                ),
                                pd.DataFrame(
                                    DF.iloc[
                                        :, [0, DF.shape[1] - 4, DF.shape[1] - 3]
                                    ]
                                ),
                                pd.DataFrame(
                                    DF.iloc[
                                        :, [0, DF.shape[1] - 2, DF.shape[1] - 1]
                                    ]
                                ),
                            )
                            tables = [DF1, DF2, DF3]
                            Soil_type = ["Low", "Medium", "High"]
                            for DF, soil_type, y in zip(tables, Soil_type, Yield):
                                DF = DF.replace("", np.nan)
                                DF.columns = ["Item", "Fix", "Var"]
                                DF["Item"] = DF["Item"].ffill()
                                DF = DF.dropna(thresh=2)
                                DF = DF.melt(
                                    id_vars=["Item"],
                                    var_name="Type",
                                    value_name="Values",
                                )
                                new_row = {
                                    "Item": "Yield",
                                    "Type": "Fix",
                                    "Values": y,
                                }
                                DF = pd.concat([DF, pd.DataFrame([new_row])], ignore_index=True)
                                DF["Values"] = DF["Values"].str.replace("$", "")
                                DF = DF.dropna(how="any")
                                DF["Item"] = DF["Item"] + " " + DF["Type"]
                                DF["Commodity"] = name
                                DF["Crop Year"] = year
                                DF["Soil Type"] = soil_type
                                Unit = []
                                for item in DF["Item"]:
                                    if item == "Yield Fix":
                                        Unit.append("bu/ac")
                                    else:
                                        Unit.append("$/ac")
                                DF["Unit"] = Unit
                                DATA = pd.concat([DF, DATA])

    DATA["Crop Year"] = DATA["Crop Year"].apply(transform_year)
    DATA = final_transformation(DATA)
    return DATA

# Execute the extraction
result = extract_data(pdf_link)
print(result)

result.to_excel("iowa.xlsx")