import copy
import json
import yahoo_fin.stock_info as si
import pymongo;
import pprint;
import datetime as date
from termcolor import colored

pp = pprint.PrettyPrinter(indent=4)
WARNING = '\033[93m'

client = pymongo.MongoClient("mongodb+srv://stockscreener:stockx@cluster0.eeh9v.mongodb.net/test?retryWrites=true&w=majority")
db = client.stocks

def parseNumberAbbreviation(numberAbbr):
  try:
    abbreviation = numberAbbr[-1:]
    number = float(numberAbbr[0:-1])
    if(abbreviation == "k"):
      return number * 1000
    elif(abbreviation == "M"):
      return number * 1000000
    elif(abbreviation == "B"):
      return number * 1000000000
    elif(abbreviation == "T"):
      return number * 1000000000000
    else:
      return number
  except Exception as e:
    print("parseNumberAbbreviation Error")
    return None

def parsePercentage(percentage):
  try:
    number = float(percentage[0:-1])
    return number * 0.01
  except Exception as e:
    print("parsePercentage Error")
    return None

def parseForwardDivAndYield(divAndYield):
  try:
    items = divAndYield.split(" ")
    div = float(items[0])
    divYield = parsePercentage(items[1][1:-1])
    return (div, divYield)
  except Exception as e:
    print("parseForwardDivAndYield Error")
    return None

def get_quote_table(symbol):
  print("quote table")
  try:
    data = si.get_quote_table(symbol)
    quote_table = {}
    quote_table["1YTargetEstimate"] = data.pop("1y Target Est")
    quote_table["52WeekRange"] = data.pop("52 Week Range")
    quote_table["ask"] = data.pop("Ask")
    quote_table["avgVolume"] = data.pop("Avg. Volume")
    quote_table["beta5YMonthly"] = data.pop("Beta (5Y Monthly)")
    quote_table["bid"] = data.pop("Bid")
    quote_table["eps"] = data.pop("EPS (TTM)")
    quote_table["earningsDate"] = data.pop("Earnings Date")
    quote_table["exDividendDate"] = data.pop("Ex-Dividend Date")
    forwardDivAndYield = parseForwardDivAndYield(data.pop("Forward Dividend & Yield"))
    quote_table["forwardDividend"] = forwardDivAndYield[0] if forwardDivAndYield is not None else None
    quote_table["forwardDividendYield"] = forwardDivAndYield[1] if forwardDivAndYield is not None else None
    quote_table["marketCap"] = parseNumberAbbreviation(data.pop("Market Cap"))
    quote_table["open"] = data.pop("Open")
    quote_table["peRatio"] = data.pop("PE Ratio (TTM)")
    quote_table["previousClose"] = data.pop("Previous Close")
    quote_table["quotePrice"] = data.pop("Quote Price")
    quote_table["volume"] = data.pop("Volume")
    print("done")
    return quote_table
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))
  
def get_stats(symbol):
  print("stats")
  try:
    data = si.get_stats(symbol)
    data.iloc[:,0] = [
      "marketCap",
      "enterpriseValue",
      "trailingPE",
      "forwardPE",
      "pegRatio",
      "priceToSales",
      "priceToBook",
      "enterPriseValueToRevenue",
      "enterPriseValueToEbitda",
      "beta5YMonthly",
      "52WeekChange",
      "S&P50052WeekChange",
      "52WeekHigh",
      "52WeekLow",
      "50DayMovingAverage",
      "200DayMovingAverage",
      "avgVol3Month",
      "avgVol10Day",
      "sharesOutstanding",
      "float",
      "percentageHeldbyInsiders",
      "percentageHeldByInstitutions",
      "sharesShort",
      "shortRatio",
      "shortPercentageOfFloat",
      "shortPercentageOfSharesOutstanding",
      "sharesShortPrevMonth",
      "forwardAnnualDividendRate",
      "forwardAnnualDividendYield",
      "trailingAnnualDividendRate",
      "trailingAnnualDividendYield",
      "5YearAverageDividendYield",
      "payOutRatio",
      "dividendDate",
      "exDividendDate",
      "lastSplitFactor",
      "lastSplitDate",
      "fiscalYearEnds",
      "mostRecentQuarter",
      "profitMargin",
      "operatingMargin",
      "returnOnAssets",
      "returnOnEquity",
      "revenue",
      "revenuePerShare",
      "quarterlyRevenueGrowth",
      "grossProfit",
      "ebitda",
      "netIncomeAviToCommon",
      "dilutedEps",
      "quarterlyEarningsGrowth",
      "totalCash",
      "totalCashPerShare",
      "totalDebt",
      "totalDebtPerEquity",
      "currentRatio",
      "bookValuePerShare",
      "operatingCashFlow",
      "leveredFreeCashFlow",
    ]
    stats = dict(zip(data.iloc[:,0], data.iloc[:,1]))

    numberAbbrKeys = [
      "marketCap",
      "enterpriseValue",
      "avgVol3Month",
      "avgVol10Day",
      "sharesOutstanding",
      "float",
      "sharesShort",
      "sharesShortPrevMonth",
      "revenue",
      "grossProfit",
      "ebitda",
      "netIncomeAviToCommon",
      "totalCash",
      "totalDebt",
      "operatingCashFlow",
      "leveredFreeCashFlow"
    ]
    for numberAbbrKey in numberAbbrKeys:
      try:
        stats[numberAbbrKey] = parseNumberAbbreviation(stats[numberAbbrKey])
      except:
        stats[numberAbbrKey] = None

    floatKeys = [
      "trailingPE",
      "forwardPE",
      "pegRatio",
      "priceToSales",
      "priceToBook",
      "enterPriseValueToRevenue",
      "enterPriseValueToEbitda",
      "beta5YMonthly",
      "52WeekHigh",
      "52WeekLow",
      "50DayMovingAverage",
      "200DayMovingAverage",
      "shortRatio",
      "forwardAnnualDividendRate",
      "trailingAnnualDividendRate",
      "5YearAverageDividendYield",
      "revenuePerShare",
      "dilutedEps",
      "totalCashPerShare",
      "totalDebtPerEquity",
      "currentRatio",
      "bookValuePerShare",
    ]
    for floatKey in floatKeys:
      try:
        stats[floatKey] = float(stats[floatKey])
      except:
        stats[floatKey] = None

    percentageKeys = [
      "52WeekChange",
      "S&P50052WeekChange",
      "percentageHeldbyInsiders",
      "percentageHeldByInstitutions",
      "shortPercentageOfFloat",
      "shortPercentageOfSharesOutstanding",
      "forwardAnnualDividendYield",
      "trailingAnnualDividendYield",
      "payOutRatio",
      "profitMargin",
      "operatingMargin",
      "returnOnAssets",
      "returnOnEquity",
      "quarterlyRevenueGrowth",
      "quarterlyEarningsGrowth",
    ]
    for percentageKey in percentageKeys:
      try:
        stats[percentageKey] = parsePercentage(stats[percentageKey])
      except:
        stats[percentageKey] = None

    print("done")
    return stats
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))

def get_analysts_info(symbol):
  try:
    print("analysts info")
    data = si.get_analysts_info(symbol)
    growthEst = data["Growth Estimates"]
    growthEst.iloc[:,0] = [
      "currentQuarter",
      "nextQuarter",
      "currentYear",
      "nextYear",
      "nextFiveYearsAnnualized",
      "pastFiveYearsAnnualized"
    ]
    analysts_info = dict(zip(growthEst.iloc[:,0], growthEst.iloc[:,1]))

    analysts_info["currentQuarter"] = parsePercentage(analysts_info["currentQuarter"])
    analysts_info["nextQuarter"] = parsePercentage(analysts_info["nextQuarter"])
    analysts_info["currentYear"] = parsePercentage(analysts_info["currentYear"])
    analysts_info["nextYear"] = parsePercentage(analysts_info["nextYear"])
    analysts_info["nextFiveYearsAnnualized"] = parsePercentage(analysts_info["nextFiveYearsAnnualized"])
    analysts_info["pastFiveYearsAnnualized"] = parsePercentage(analysts_info["pastFiveYearsAnnualized"])

    print("done")
    return analysts_info
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))

def get_dividends(symbol):
  print("dividends")
  try:
    data = si.get_dividends(symbol)
    data.index = data.index.strftime("%Y-%m-%d")    
    dividends = data.to_dict()['dividend']
    print("done")
    return dividends
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))

def get_earnings(symbol):
  print("earnings")
  try:
    data = si.get_earnings(symbol)
    earnings = {}
    earnings["quarterly_results"] = data["quarterly_results"].to_dict("records")
    earnings["quarterly_revenue_earnings"] = data["quarterly_revenue_earnings"].to_dict("records")
    earnings["yearly_revenue_earnings"] = data["yearly_revenue_earnings"].to_dict("records")
    print("done")
    return earnings
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))

def get_financials(symbol):
  print("financials")
  try:
    data = si.get_financials(symbol)
    yearly_income_statement = data_frame_to_dict(data["yearly_income_statement"])
    quarterly_income_statement = data_frame_to_dict(data["yearly_income_statement"])
    yearly_balance_sheet = data_frame_to_dict(data["yearly_balance_sheet"])
    quarterly_balance_sheet = data_frame_to_dict(data["quarterly_balance_sheet"])
    yearly_cash_flow = data_frame_to_dict(data["yearly_cash_flow"])
    quarterly_cash_flow = data_frame_to_dict(data["quarterly_cash_flow"])
    financials = {
      "quarterly_balance_sheet": quarterly_balance_sheet,
      "quarterly_cash_flow": quarterly_cash_flow,
      "quarterly_income_statement": quarterly_income_statement,
      "yearly_balance_sheet": yearly_balance_sheet,
      "yearly_cash_flow": yearly_cash_flow,
      "yearly_income_statement": yearly_income_statement,
    }
    print("done")
    return financials
  except Exception as e:
    print(colored("Error:", "red")) 
    print(colored(e, "red"))

def data_frame_to_dict(dataFrame):
  dataFrame.columns = dataFrame.columns.astype(str)
  return dataFrame.to_dict("index")

def get_fundamentals(symbol):
  fundamentals = {
    "symbol": symbol,
    "quote_table": get_quote_table(symbol),
    "stats": get_stats(symbol),
    "analysts_info": get_analysts_info(symbol),
    "dividends": get_dividends(symbol),
    "earnings": get_earnings(symbol),
    "financials": get_financials(symbol),
    "lastUpdated": date.datetime.now()
  }
  return fundamentals

with open ('symbols.json') as symbols_file:
  file = json.load(symbols_file)
  symbols = file["data"]["symbols"]

  items = list(db.fundamentals.find({}).batch_size(6000))
  count = len(items)
  counter = 0
  for item in list(items):
    print("------------------------------------")
    print(f'{counter + 1} of {count}')
    if(item is None):
      print("Fetching " + item["symbol"])
      item_fundamentals = get_fundamentals(item["symbol"])
      db.fundamentals.insert_one()
    else:
      print("Updating " + item["symbol"])
      item_fundamentals = get_fundamentals(item["symbol"])

      name = ""
      for symbol in symbols:
        if(symbol["symbol"] == item["symbol"]):
          name = symbol["name"]

      updatedValues = {
        "$set": {
          "name": name,
          "quote_table": item_fundamentals["quote_table"],
          "stats": item_fundamentals["stats"],
          "analysts_info": item_fundamentals["analysts_info"],
          "dividends": item_fundamentals["dividends"],
          "earnings": item_fundamentals["earnings"],
          "financials": item_fundamentals["financials"],
          "lastUpdated": date.datetime.now()
        }
      }
      db.fundamentals.update_one(
        {"symbol": item["symbol"]},
        updatedValues
      )
    counter += 1
    print("------------------------------------")

# with open ('symbols.json') as symbols_file:
#   file = json.load(symbols_file)
#   symbols = file["data"]["symbols"]
#   fundamentals = []
#   counter = 0
#   for entry in symbols:
#     print(f'{counter + 1} of {len(symbols)}')
#     print("Name: " + entry['name'])
#     try:
#       results = db.fundamentals.find_one({
#         "symbol": entry['symbol']
#       })
#       if(results is None):
#         print("inserting" + entry['symbol'])
#         stock_fundamentals = get_fundamentals(entry['symbol'])   
#         fundamentals.append(copy.deepcopy(stock_fundamentals))
#         db.fundamentals.insert_one(stock_fundamentals)
#       else:
#         fundamentals.append(results)
#     except Exception as e:
#       print(colored("Error:", "red")) 
#       print(colored(e, "red"))
#       print(colored("base data faulty", "red"))
#     counter += 1
#   with open("fundamentals.json", "w") as fundamentals_file:
#     fundamentals_json = json.dumps(fundamentals, indent=4)
#     fundamentals_file.write(fundamentals_json)