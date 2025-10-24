import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_link(url):
    """
    Extract the Excel file link from the given URL.
    """
    try:
        page = requests.get(url)
        page.raise_for_status()  # Raise an error for bad status codes
        logger.info("Successfully fetched the webpage.")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch the URL: {e}")
        raise ValueError("The URL provided is not working")

    soup = BeautifulSoup(page.text, "html.parser")
    ul_tags = soup.find_all("ul")

    links = []
    for ul_tag in ul_tags:
        a_tags = ul_tag.find_all("a")
        for a_tag in a_tags:
            href = a_tag.get("href")
            if href:
                links.append(href)

    for link in links:
        if link.endswith((".xlsx", ".xls", ".xlsm")) and "Crop" in link:
            logger.info(f"Found Excel link: {link}")
            return link

    logger.error("No suitable Excel link found.")
    raise ValueError("No Excel file link found on the page")

def get_yield_price(url, sheet):
    """
    Get the yield and price for each crop from the specified sheet.
    """
    try:
        Y_P = pd.read_excel(url, sheet_name=sheet, usecols="C,F,G", skiprows=2, nrows=1)
        Y_P = Y_P.dropna()
        Y_P = Y_P.rename(columns={Y_P.columns[0]: "Item", Y_P.columns[1]: "Yield", Y_P.columns[2]: "Price"})
        DF_melt = (
            pd.melt(
                Y_P,
                id_vars=["Item"],
                value_vars=["Yield", "Price"],
                ignore_index=False,
            )
            .reset_index()
            .drop("index", axis=1)
        )
        DF_melt = DF_melt.drop('Item', axis=1)
        DF_melt = DF_melt.rename(columns={DF_melt.columns[0]: "Item", DF_melt.columns[1]: "Value"})
        Unit = ['bu/ac', '$/bu']
        DF_melt['Unit'] = Unit
        return DF_melt
    except Exception as e:
        logger.error(f"Error processing yield and price for sheet {sheet}: {e}")
        return pd.DataFrame()

def main():
    url = "https://arec.tennessee.edu/extension/budgets/"
    try:
        excel_href = extract_link(url)
    except ValueError as e:
        logger.error(e)
        return

    crop = ["corn", "soybeans", "cotton", "wheat", "sorghum"]
    try:
        Year = int(excel_href.split("-Crop-Budgets.xlsm")[0][-4:])
        logger.info(f"Extracted year: {Year}")
    except (IndexError, ValueError) as e:
        logger.error(f"Failed to extract year from link: {e}")
        return

    try:
        excel = pd.ExcelFile(excel_href)
        Sheets = []
        for sheet in excel.sheet_names:
            for commodity in crop:
                if commodity in sheet.lower():
                    Sheets.append(sheet)
                    break  # Avoid duplicates
        logger.info(f"Relevant sheets: {Sheets}")
    except Exception as e:
        logger.error(f"Error loading Excel file: {e}")
        return

    Files = [excel_href, str(Year), Sheets]

    Data = pd.DataFrame()
    for sheet in Files[2]:
        try:
            DF = pd.read_excel(Files[0], sheet_name=sheet, usecols="C,H,E", skiprows=1)
            DF = DF.dropna()
            DF = DF.rename(columns={DF.columns[0]: "Item", DF.columns[1]: "Unit", DF.columns[2]: "Value"})
            DF['Unit'] = '$/ac'
            Y_P = get_yield_price(Files[0], sheet)
            DF = pd.concat([DF, Y_P])
            DF = DF.assign(Commodity=sheet, Location='Tennessee', Year=Files[1], Source='arec.tennessee')
            Data = pd.concat([Data, DF])
        except Exception as e:
            logger.error(f"Error processing sheet {sheet}: {e}")

    def transform_year(year):
        """
        Transform the year to a range format, e.g., 2022 to 2022/2023.
        """
        try:
            year = int(year)
            return f"{year}/{year+1}"
        except ValueError:
            return year

    Data["Year"] = Data["Year"].apply(transform_year)

    try:
        output_path = 'tenessee.xlsx'
        Data.to_excel(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving data to Excel: {e}")

if __name__ == "__main__":
    main()
