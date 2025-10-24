import pandas as pd
from bs4 import BeautifulSoup
import requests
import tabula.io as tb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration dictionary for hardcoded values
CONFIG = {
    'url': "https://ag.purdue.edu/commercialag/home/resource/keyword/crop-cost-return-guide-archive/",
    'pdf_area_page1': (200, 48, 450, 1400),
    'pdf_area_page3': (120, 0, 250, 800),
    'productivity_mapping': {
        0: "Productivity",
        1: "Low", 2: "Low", 3: "Low", 4: "Low",
        5: "Average", 6: "Average", 7: "Average", 8: "Average",
        9: "High", 10: "High", 11: "High", 12: "High",
    },
    'value_vars': [
        "Market revenue", "Fertilizer5", "Seed6", "Pesticides7", "Dryer fuel8",
        "Machinery fuel ", "Machinery repairs9", "Hauling10", "Interest11",
        "Insurance/misc.12", "Total variable cost", "Expected yield per acre2", "Harvest price3"
    ],
    'location': "Indiana",
    'source': "Purdue"
}

def transform_year(year):
    """
    Transform the year from '2022' to '2022/2023'.
    """
    try:
        year_int = int(year)
        return f"{year_int}/{year_int + 1}"
    except ValueError:
        logger.error(f"Invalid year format: {year}")
        return year

def extract_path(url):
    """
    Scrape the report links for each year from the given URL.
    """
    try:
        page = requests.get(url)
        page.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch URL {url}: {e}")
        raise

    soup = BeautifulSoup(page.text, "html.parser")
    elements = soup.find_all(class_="fl-post-column")

    links = []
    for element in elements:
        references = element.find_all("a")
        for ref in references:
            links.append(ref["href"])

    links_years = []
    for link in links:
        year = link.split("-crop-cost-and-return-guide/")[0][-4:]
        try:
            int(year)
        except ValueError:
            year = link.split("resource/")[1].split("/")[0]
        links_years.append([year, link])

    return links_years

def find_href_into_path(url):
    """
    Find the PDF link for the latest year.
    """
    links_years = extract_path(url)
    pdf_files = []

    for year_link in links_years:
        try:
            page = requests.get(year_link[1])
            page.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch year page {year_link[1]}: {e}")
            continue

        soup = BeautifulSoup(page.text, "html.parser")
        elements = soup.find_all(
            class_="fl-button-wrap fl-button-width-full fl-button-center fl-button-has-icon"
        )
        for element in elements:
            references = element.find_all("a")
            for ref in references:
                pdf_files.append([year_link[0], ref["href"]])

    if not pdf_files:
        raise ValueError("No PDF links found")

    return pdf_files[0]  # Return the first (latest) one

def extract_indiana_crop_budget(pdf_link):
    """
    Extract and process crop budget data from the PDF.
    """
    try:
        df = tb.read_pdf(
            pdf_link[1],
            pages="1",
            area=CONFIG['pdf_area_page1'],
            pandas_options={"header": None},
            stream=True,
        )[0]
    except Exception as e:
        logger.error(f"Failed to read PDF {pdf_link[1]}: {e}")
        raise

    # Clean and transform the dataframe
    df[0] = df[0].str.split("@").str[0]
    df.iat[0, 0] = "Rotation"
    df.iat[1, 0] = "Commodity"
    df.iat[0, 4] = "N/A"
    df.iat[0, 9] = "N/A"
    df.iat[0, 14] = "N/A"

    df = df.dropna()
    df = df.transpose()
    df = df.set_axis(df.iloc[0], axis=1)
    df = df[1:]
    df = df.drop("per acre", axis=1, errors='ignore')

    # Add productivity soil
    productivity_soil = (["Low"] * 5 + ["Average"] * 5 + ["High"] * 5)
    df["Productivity soil"] = productivity_soil

    # Melt the dataframe
    df_melted = pd.melt(
        df,
        id_vars=["Productivity soil", "Rotation", "Commodity"],
        value_vars=CONFIG['value_vars'],
        ignore_index=False,
    ).reset_index().drop("index", axis=1)

    df_melted = df_melted.rename(columns={df_melted.columns[3]: "Item"}).assign(
        Location=CONFIG['location'], Year=pdf_link[0]
    )

    return df_melted

def indiana_crop_budget(pdf_link):
    """
    Process the Indiana crop budget data, adding units and cleaning values.
    """
    df = extract_indiana_crop_budget(pdf_link)

    # Determine units
    units = []
    for item in df["Item"]:
        if "price" in item.lower():
            units.append("dollars/bushel")
        elif "yield" in item.lower():
            units.append("bushels/acre")
        else:
            units.append("dollars/acre")
    df["Unit"] = units

    df = df.assign(Source=CONFIG['source'])
    df = df.rename(columns={df.columns[5]: "Value2"})
    df["value"] = df["value"].str.replace(r"\$", "", regex=True)

    return df.reset_index(drop=True)

def scrap_fixed_cost(pdf_link):
    """
    Scrape and process fixed cost data from page 3 of the PDF.
    """
    try:
        df = tb.read_pdf(
            pdf_link[1],
            pages="3",
            area=CONFIG['pdf_area_page3'],
            pandas_options={"header": None},
            stream=True,
        )[0]
    except Exception as e:
        logger.error(f"Failed to read PDF page 3 {pdf_link[1]}: {e}")
        raise

    df = df.rename(columns=CONFIG['productivity_mapping'])
    df = df.T
    df = df.reset_index()
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df = df.dropna(axis=1)
    df = df.replace(r"\$", "", regex=True)

    # Convert to float
    numeric_cols = ["Crop contribution margin2", "Total contribution margin",
                    "Machinery ownership4", "Family and hired labor5", "Land6", "Earnings or (losses)"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Group and melt
    grouped = df.groupby(["Rotation1", "Productivity"]).mean()
    grouped = grouped.reset_index()
    df_melted = pd.melt(
        grouped,
        id_vars=["Rotation1", "Productivity"],
        var_name="Item",
        value_name="Value",
    )

    df_melted = df_melted.rename(columns={
        "Rotation1": "Rotation",
        "Productivity": "Productivity soil",
        "Value": "value"
    })

    df_melted["Location"] = CONFIG['location']
    df_melted["Year"] = pdf_link[0]
    df_melted["Commodity"] = "Corn"
    df_melted["Unit"] = "dollars/acre"  # Assuming based on context
    df_melted["Source"] = CONFIG['source']
    df_melted["Value2"] = df_melted["Location"]  # Adjust as needed

    # Transform rotation
    df_melted["Rotation"] = df_melted["Rotation"].map({"c-c": "Cont.", "c-b": "Rot."}).fillna(df_melted["Rotation"])

    return df_melted.reset_index(drop=True)

def main():
    """
    Main function to run the scraping and processing.
    """
    logger.info("Starting Indiana crop budget scraping.")

    try:
        pdf_link = find_href_into_path(CONFIG['url'])
        logger.info(f"Found PDF link: {pdf_link}")

        df_indiana = indiana_crop_budget(pdf_link)
        df_indiana["Year"] = df_indiana["Year"].apply(transform_year)
        df_indiana = df_indiana[df_indiana["value"] != 'N/A']

        # Optionally scrape fixed costs
        # fixed_df = scrap_fixed_cost(pdf_link)
        # combined_df = pd.concat([df_indiana, fixed_df])

        df_indiana.to_excel("indiana_output.xlsx", index=False)
        logger.info("Data exported to indiana_output.xlsx")

        print("Unique commodities:", df_indiana["Commodity"].unique())
        print("Unique values:", df_indiana["value"].unique())

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
