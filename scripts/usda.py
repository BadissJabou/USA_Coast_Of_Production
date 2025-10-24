import pandas as pd
import requests
from bs4 import BeautifulSoup

def scrape_links(url, commodities):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    result = []
    for comm in commodities:
        for link in links:
            if comm.lower() in link['href'].lower() and ('xlsx' in link['href'] or 'xls' in link['href']):
                result.append(link['href'])
                break
    return result

def download_file(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)

def process_recent_data(url):
    # Scrape and download recent corn and soybeans data
    links = scrape_links(url, ['corn', 'soybeans'])
    base_url = "https://www.ers.usda.gov"
    corn_url = base_url + links[0]
    soy_url = base_url + links[1]
    download_file(corn_url, 'corn.xlsx')
    download_file(soy_url, 'soybeans.xlsx')

    # Process corn data
    corn = pd.read_excel('corn.xlsx', skiprows=4, header=1)
    corn = corn.dropna(how='all')
    corn = corn.iloc[1:]
    corn.columns = ['Item'] + [str(int(x)) for x in corn.iloc[0, 1:].fillna(0)]
    corn = corn.iloc[1:]
    corn = corn.set_index('Item')
    corn = corn.T
    corn['Year'] = corn.index
    corn = corn.reset_index(drop=True)
    corn = corn[['Year', 'Primary product, grain', 'Secondary product, silage', 'Total, gross value of production', 'Total, operating costs', 'Total, allocated overhead', 'Total, costs listed', 'Net value']]
    corn['Commodity'] = 'Corn'

    # Process soybeans data
    soybeans = pd.read_excel('soybeans.xlsx', skiprows=4, header=1)
    soybeans = soybeans.dropna(how='all')
    soybeans = soybeans.iloc[1:]
    soybeans.columns = ['Item'] + [str(int(x)) for x in soybeans.iloc[0, 1:].fillna(0)]
    soybeans = soybeans.iloc[1:]
    soybeans = soybeans.set_index('Item')
    soybeans = soybeans.T
    soybeans['Year'] = soybeans.index
    soybeans = soybeans.reset_index(drop=True)
    soybeans = soybeans[['Year', 'Primary product, soybeans', 'Total, gross value of production', 'Total, operating costs', 'Total, allocated overhead', 'Total, costs listed', 'Net value']]
    soybeans['Commodity'] = 'Soybeans'

    return pd.concat([corn, soybeans], ignore_index=True)

def process_historical_data(url):
    # Scrape and download historical data
    links = scrape_links(url, ['us-1975-95', 'us-1975-96'])
    base_url = "https://www.ers.usda.gov"
    hist_corn_url = base_url + links[0]
    hist_soy_url = base_url + links[1]
    download_file(hist_corn_url, 'us-1975-95.xls')
    download_file(hist_soy_url, 'us-1975-96.xls')

    # Process historical corn
    hist_corn = pd.read_excel('us-1975-95.xls', skiprows=4, header=1)
    hist_corn = hist_corn.dropna(how='all')
    hist_corn = hist_corn.iloc[1:]
    hist_corn.columns = ['Item'] + [str(int(x)) for x in hist_corn.iloc[0, 1:].fillna(0)]
    hist_corn = hist_corn.iloc[1:]
    hist_corn = hist_corn.set_index('Item')
    hist_corn = hist_corn.T
    hist_corn['Year'] = hist_corn.index
    hist_corn = hist_corn.reset_index(drop=True)
    hist_corn = hist_corn[['Year', '  Corn grain', '  Corn silage', '    Total, gross value of production', '    Total, cash expenses', '    Subtotal', '  Residual returns to risk and management  ']]
    hist_corn['Commodity'] = 'Corn'

    # Process historical soybeans
    hist_soy = pd.read_excel('us-1975-96.xls', skiprows=4, header=1)
    hist_soy = hist_soy.dropna(how='all')
    hist_soy = hist_soy.iloc[1:]
    hist_soy.columns = ['Item'] + [str(int(x)) for x in hist_soy.iloc[0, 1:].fillna(0)]
    hist_soy = hist_soy.iloc[1:]
    hist_soy = hist_soy.set_index('Item')
    hist_soy = hist_soy.T
    hist_soy['Year'] = hist_soy.index
    hist_soy = hist_soy.reset_index(drop=True)
    hist_soy = hist_soy[['Year', '  Soybeans', '    Total, gross value of production', '      Total, cash expenses', '    Total, economic costs', '  Residual returns to management and risk']]
    hist_soy['Commodity'] = 'Soybeans'

    return pd.concat([hist_corn, hist_soy], ignore_index=True)

def process_forecast_data(url):
    # Scrape and download forecast data
    links = scrape_links(url, ['cost-of-production-forecasts-for-major-us-field-crops'])
    base_url = "https://www.ers.usda.gov"
    forecast_url = base_url + links[0]
    download_file(forecast_url, 'forecast.xlsx')

    # Process forecast
    forecast = pd.read_excel('forecast.xlsx', skiprows=2, header=1)
    forecast = forecast.dropna(how='all')
    forecast = forecast.iloc[1:]
    forecast.columns = ['Item'] + [str(int(x)) for x in forecast.iloc[0, 1:].fillna(0)]
    forecast = forecast.iloc[1:]
    forecast = forecast.set_index('Item')
    forecast = forecast.T
    forecast['Year'] = forecast.index
    forecast = forecast.reset_index(drop=True)
    forecast = forecast[['Year', '      Total, operating costs', '      Total, allocated costs', '      Total, costs listed']]
    forecast['Commodity'] = 'Forecast'

    return forecast

def main():
    url = "https://www.ers.usda.gov/data-products/commodity-costs-and-returns/"

    try:
        # Extract recent data
        Recent = process_recent_data(url)

        # Extract historical data
        F_Histo = process_historical_data(url)

        # Extract forecast data
        Forecast = process_forecast_data(url)

        # Concatenate all data
        Data = pd.concat([Recent, F_Histo, Forecast], ignore_index=True)

        # Check unique years
        print("Unique years:", Data["Year"].unique())

        # Save to Excel
        Data.to_excel('USDA.xlsx', index=None)
        print("Data saved to USDA.xlsx")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
