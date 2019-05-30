#!python2

import blpapi
from blpapi import Event as EventType
import csv
from datetime import date, datetime
import re

mainDir = "S:\\Investment\\Arb"
brDataDir = "S:\\Trading\SFTP\Broadridge\Arbitrage"
dataDir = mainDir + "\\Data"
portDir = mainDir + "\\Portfolios"

EVENT_TYPE_NAMES = {
    EventType.ADMIN: "ADMIN",
    EventType.SESSION_STATUS: "SESSION_STATUS",
    EventType.SUBSCRIPTION_STATUS: "SUBSCRIPTION_STATUS",
    EventType.REQUEST_STATUS: "REQUEST_STATUS",
    EventType.RESPONSE: "RESPONSE",
    EventType.PARTIAL_RESPONSE: "PARTIAL_RESPONSE",
    EventType.SUBSCRIPTION_DATA: "SUBSCRIPTION_DATA",
    EventType.SERVICE_STATUS: "SERVICE_STATUS",
    EventType.TIMEOUT: "TIMEOUT",
    EventType.AUTHORIZATION_STATUS: "AUTHORIZATION_STATUS",
    EventType.RESOLUTION_STATUS: "RESOLUTION_STATUS",
    EventType.TOPIC_STATUS: "TOPIC_STATUS",
    EventType.TOKEN_STATUS: "TOKEN_STATUS",
    EventType.REQUEST: "REQUEST"
}

BBG_DATA = blpapi.Name("data")
BBG_ERROR_INFO = blpapi.Name("errorInfo")
BBG_FIELD_DATA = blpapi.Name("fieldData")
BBG_FIELD_ID = blpapi.Name("fieldId")
BBG_FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
BBG_OVERRIDES = blpapi.Name("overrides")
BBG_SECURITY = blpapi.Name("security")
BBG_SECURITY_DATA = blpapi.Name("securityData")

bbgHost = "localhost"
bbgPort = 8194
bbgService = "//blp/refdata"

def LoadPriceOverrides():
    priceOverrides = {}

    try:
        fileName = dataDir + "\\PRICE_OVERRIDES.txt"

        with open(fileName) as file:
            header = []

            for line in file:
                if (line.startswith("#")):
                    # header needs to be the first row.
                    if (not header):
                        header = line.lstrip("#").rstrip("\n").split("|")
                else:
                    if (not header):
                        return None

                    priceOverride = dict(zip(header, line.rstrip("\n").split("|")))
                    priceOverride["TO_DATE"] = datetime.strptime(priceOverride["TO_DATE"], \
                                                                 "%Y%m%d").date()
                    priceOverride["PRICE"] = float(priceOverride["PRICE"])

                    priceOverrides[priceOverride["BBG_TICKER"]] = priceOverride


    except IOError:
        return None

    return priceOverrides


def LoadPortfolioData():
    portData = {}

    try:
        fileName = dataDir + "\\PORTFOLIO_DATA.txt"

        with open(fileName) as file:
            header = []

            for line in file:
                if (line.startswith("#")):
                    # header needs to be the first row.
                    if (not header):
                        header = line.lstrip("#").rstrip("\n").split("|")
                else:
                    if (not header):
                        return None

                    portDataEntry = dict(zip(header, line.rstrip("\n").split("|")))
                    portData[portDataEntry['PORTFOLIO']] = portDataEntry


    except IOError:
        return None

    return portData


def GetPortfolioConsts(portName, date = None):
    port = {}

    try:
        fileName = portDir + "\\" + portName.upper() + "_const.txt"

        if (date):
            fileName = portDir + "\\Temp\\" + portName.upper() + "_const_" + date.strftime("%Y%m%d") + ".txt"

        with open(fileName) as file:
            reader = csv.reader(file, delimiter = "|")

            for line in reader:
                if (len(line) != 2 or line[0].startswith("#")):
                    continue

                ticker = line[0]
                qty = float(line[1])

                port[ticker] = qty

    except IOError:
        return None

    return port


def GetPortfolioDiv(portName, date = None):
    try:
        fileName = portDir + "\\" + portName.upper() + "_div.txt"

        if (date):
            fileName = portDir + "\\Temp\\" + portName.upper() + "_div_" + date.strftime("%Y%m%d") + \
                       ".txt"

        with open(fileName) as file:
            for line in file:
                if (line.startswith("#")):
                    continue

                divisor = float(line)

    except IOError:
        return None

    return divisor


def GetPortfolioCash(portName, date = None):
    cash = {}

    try:
        fileName = portDir + "\\" + portName + "_cash.txt"

        if (date):
            fileName = portDir + "\\Temp\\" + portName + "_cash_" + date.strftime("%Y%m%d") + ".txt"

        with open(fileName) as file:
            reader = csv.reader(file, delimiter = "|")

            for line in reader:
                if (line[0].startswith("#")):
                    continue

                curr = line[0]
                qty = float(line[1])

                cash[curr] = qty

    except IOError:
        return None

    return cash


def GetConfig(fileName, cfgKeys):
    cfg = {}

    try:
        with open(fileName) as file:
            header = []

            for line in file:
                if (line.startswith("#")):
                    # header needs to be the first row.
                    if (not header):
                        header = line.lstrip("#").rstrip("\n").split("|")

                        for cfgKey in cfgKeys:
                            if (cfgKey not in header):
                                return None
                else:
                    if (not header):
                        return None

                    cfgData = dict(zip(header, line.rstrip("\n").split("|")))
                    cfgKey = "-".join([cfgData[x] for x in cfgKeys])
                    cfg[cfgKey] = cfgData

    except IOError:
        return None

    return cfg

def GetBBGBeqsRequest(bbgSession, eqsScreens):
    data = []

    bbgRefDataService = bbgSession.getService(bbgService)
    bbgRequest = bbgRefDataService.createRequest("BeqsRequest")
    bbgRequest.set("screenType", "PRIVATE")

    for eqsScreen in eqsScreens:
        bbgRequest.set("screenName", eqsScreen)

        bbgSession.sendRequest(bbgRequest)

        try:
            while (True):
                event = bbgSession.nextEvent(500)

                if (event.eventType() == EventType.PARTIAL_RESPONSE or event.eventType() == EventType.RESPONSE):
                    for msg in event:
                        if (not msg.hasElement(BBG_DATA) or not msg.getElement(BBG_DATA).hasElement(BBG_SECURITY_DATA)):
                            continue

                        for msgSecurity in msg.getElement(BBG_DATA).getElement(BBG_SECURITY_DATA).values():
                            # append BBG security.
                            data.append(msgSecurity.getElementAsString(BBG_SECURITY))

                    if (event.eventType() == EventType.RESPONSE):
                        break
#                else:
#                    ProcessBBGEvent(event)

        except:
            raise

    return data

def GetBBGReferenceDataRequest(session, securities, fields):
    data = {}

    refDataService = session.getService(bbgService)
    request = refDataService.createRequest("ReferenceDataRequest")

    for security in securities:
        request.append("securities", security)

    for field in fields:
        request.append("fields", field)

    session.sendRequest(request)

    try:
        while (True):
            event = session.nextEvent(500)

            if (event.eventType() == blpapi.Event.PARTIAL_RESPONSE or event.eventType() == blpapi.Event.RESPONSE):
                for msg in event:
#                    print(str(msg)[:1024])
#                    print(str(msg)[-1024:])

                    if (not msg.hasElement(BBG_SECURITY_DATA)):
                        continue

                    for msgSecurity in msg.getElement(BBG_SECURITY_DATA).values():
                        bbgSecurity = msgSecurity.getElementAsString(BBG_SECURITY)
                        fields = msgSecurity.getElement(BBG_FIELD_DATA)

                        if (fields.numElements() < 1):
                            continue

                        data[bbgSecurity] = {}

                        for field in fields.elements():
                            if (not field.isValid()):
                                continue

                            fieldName = str(field.name())

                            if (field.isArray()):
                                data[bbgSecurity][fieldName] = []

                                # set each element to a list of values.
                                for value in field.values():
                                    elems = []

                                    for elem in value.elements():
                                        if (elem.isValid() and not elem.isNull()):
                                            elems.append(elem.getValue())
                                        else:
                                            elems.append("")

                                    data[bbgSecurity][fieldName].append(elems)
                            else:
                                data[bbgSecurity][fieldName] = field.getValue()

                if (event.eventType() == blpapi.Event.RESPONSE):
                    break
#            else:
#                ProcessBBGEvent(event)

    except:
        raise

    return data

def ProcessBBGEvent(event):
    print("{}:".format(EVENT_TYPE_NAMES[event.eventType()]))

    for msg in event:
        print(msg)

def GetBloombergSecurity(bbgCode):
    bbgCodeParts = bbgCode.split(" ")

    if (len(bbgCodeParts) == 1):
        if (len(bbgCodeParts[0]) == 3 and re.search("[A-Z][A-Z][A-Z]", bbgCodeParts[0])):
            return bbgCode + " Curncy"
        else:
            futSeries = bbgCodeParts[0][:-2]
            futMonth = bbgCodeParts[0][-2:-1]
            futYear = bbgCodeParts[0][-1:]

            if (re.search("[FGHJKMNQUVXZ]", futMonth) and re.search("[0-9]", futYear)):
                if (futSeries == "UCA" or futSeries == "XUC"):
                    return futSeries + " Curncy"
                else:
                    return futSeries + " Index"
    elif (len(bbgCodeParts) == 2 and re.search("[A-Z][A-Z0-9]", bbgCodeParts[1])):
        return bbgCode + " Equity"

    return ""

def GetBRData(date, mode):
    brData = []

    try:
        fileName = brDataDir + "\\Ovata_Port_" + mode + "_" + date.strftime("%Y%m%d") + ".csv"
        
        with open(fileName) as file:
            header = None;

            reader = csv.reader(file, delimiter = ",")

            for line in reader:
                # the first line contains the field headers.  use them as the keys for the dictionary.
                if (header == None):
                    header = line
                    continue

                brDataEntry = {key: value for (key, value) in zip(header, line)}

                # FX forwards do not have the maturity date field set.  parse it from the security's ticker.
                if (brDataEntry["Security Type Name"] == "FX Forward" and re.search("\d+/\d+/\d{4}", brDataEntry["Security Ticker"])):
                    brDataEntry["Security Maturity Date"] = re.search("\d+/\d+/\d{4}", brDataEntry["Security Ticker"]).group()

                # convert date fields into a date.
                if (brDataEntry["Security Maturity Date"]):
                    brDataEntry["Security Maturity Date"] = datetime.strptime(brDataEntry["Security Maturity Date"], "%m/%d/%Y").date()

                if (brDataEntry["Underlying Maturity Date"]):
                    brDataEntry["Underlying Maturity Date"] = datetime.strptime(brDataEntry["Underlying Maturity Date"], "%m/%d/%Y").date()

                if (brDataEntry["Security NDF Fixing Date"]):
                    brDataEntry["Security NDF Fixing Date"] = datetime.strptime(brDataEntry["Security NDF Fixing Date"], "%m/%d/%Y").date()

                # convert intergal/decimal numbers.
                if (brDataEntry["Position"]):
                    brDataEntry["Position"] = float(brDataEntry["Position"].replace(',"', ""))

                if (brDataEntry["Security Pricing Factor"]):
                    brDataEntry["Security Pricing Factor"] = float(brDataEntry["Security Pricing Factor"].translate(None, '",'))

                if (brDataEntry["Price"]):
                    brDataEntry["Price"] = float(brDataEntry["Price"].translate(None, '",'))

                if (brDataEntry["Base -> Sec FX"]):
                    brDataEntry["Base -> Sec FX"] = float(brDataEntry["Base -> Sec FX"].translate(None, '",'))

                if (brDataEntry["Underlying Pricing Factor"]):
                    brDataEntry["Underlying Pricing Factor"] = float(brDataEntry["Underlying Pricing Factor"].translate(None, '",'))

                if (brDataEntry["Underlying End Price"]):
                    brDataEntry["Underlying End Price"] = float(brDataEntry["Underlying End Price"].translate(None, '",'))

                if (brDataEntry["Underlying End FX"]):
                    brDataEntry["Underlying End FX"] = float(brDataEntry["Underlying End FX"].translate(None, '",'))

                # enrich data set - Custodian.
                if (brDataEntry["Custodian Code"].endswith("FO")):
                    brDataEntry["Custodian"] = brDataEntry["Custodian Code"][:brDataEntry["Custodian Code"].rfind("FO")]
                elif (brDataEntry["Custodian Code"].endswith("PB")):
                    brDataEntry["Custodian"] = brDataEntry["Custodian Code"][:brDataEntry["Custodian Code"].rfind("PB")]
                elif (brDataEntry["Custodian Code"].endswith("SWAP")):
                    brDataEntry["Custodian"] = brDataEntry["Custodian Code"][:brDataEntry["Custodian Code"].rfind("SWAP")]
                elif (brDataEntry["Custodian Code"].endswith("INT")):
                    brDataEntry["Custodian"] = brDataEntry["Custodian Code"][:brDataEntry["Custodian Code"].rfind("INT")]
                else:
                    brDataEntry["Custodian"] = brDataEntry["Custodian Code"]

                # enrich data set - Ticker/BBG Code/BBG Security.
                securityTypeField = "Security Type Name"
                tickerField = "Security Ticker"

                if (brDataEntry["Security Type Name"] == "Contract for Difference"):
                    securityTypeField = "Underlying Type Name"
                    tickerField = "Underlying Ticker"

                ticker = brDataEntry[tickerField]
                bbgCode = ""
                bbgSecurity = ""

                if (brDataEntry[securityTypeField] == "Future"):
                    ticker =  brDataEntry[tickerField].split(" ")[0]
                    bbgCode = ticker
                elif (brDataEntry[securityTypeField] == "Depository Receipt" or
                      brDataEntry[securityTypeField] == "Equity" or
                      brDataEntry[securityTypeField] == "Equity Unit" or
                      brDataEntry[securityTypeField] == "Exchange Traded Fund" or
                      brDataEntry[securityTypeField] == "Currency"):

                    bbgCode = ticker

                if (bbgCode):
                    bbgSecurity = GetBloombergSecurity(bbgCode)

                brDataEntry["Ticker"] = ticker
                brDataEntry["BBG Code"] = bbgCode
                brDataEntry["BBG Security"] = bbgSecurity

                brData.append(brDataEntry);

    except IOError:
        return None

    return brData
