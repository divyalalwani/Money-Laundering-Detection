
# creating global Variables
today = datetime.today()
datem = datetime(today.year, today.month, today.day)
threemonthsago = datem + relativedelta(months=-3)
sixmonthsago = datem + relativedelta(months=-6)
pd.set_option('display.max_columns', None)
ModifiedIndDf = pd.DataFrame(
    columns=['accountNumber', 'meanTranscPerDisctAccntPerMonth', 'sixmonthTotalamountsDeposited_OA',
             'sixmonthTotalamountsWithdrawn_OA',
             'threemonthTotalamountsDeposited_OA', 'threemonthTotalamountsWithdrawn_OA',
             'sixmonthTotalamountsDeposited_MA',
             'sixmonthTotalamountsWithdrawn_MA',
             'threemonthTotalamountsDeposited_MA', 'threemonthTotalamountsWithdrawn_MA', 'frequentDepositSum',
             'flaggedAccntVal',
             'accountDetails'])
scoring_uri = 'http://d92892b1-b1e8-4483-9bfa-4add18993127.canadacentral.azurecontainer.io/score'

#settting threshhols
threshHold_meanTranscPerDisctAccntPerMonth = 5
threshHold_sixmonthTotalamountsDeposited_OA = 50000
threshHold_sixmonthTotalamountsWithdrawn_OA = 50000
threshHold_threemonthTotalamountsDeposited_OA =25000
threshHold_threemonthTotalamountsWithdrawn_OA =25000
threshHold_sixmonthTotalamountsDeposited_MA = 50000
threshHold_sixmonthTotalamountsWithdrawn_MA = 50000
threshHold_threemonthTotalamountsDeposited_MA = 25000
threshHold_threemonthTotalamountsWithdrawn_MA =25000
threshHold_frequentDepositSum = 5000
threshHold_sixmonthsagoaccountDetailsgroupDepositeddata = 50000
threshHold_sixmonthsagoaccountDetailsgroupWithdrawndata =50000
threshHold_threemonthsagoaccountDetailsgroupDepositeddata = 25000
threshHold_threemonthsagoaccountDetailsgroupWithdrawndata = 25000
threshHold_flaggedAccntVal = 0

def sqlconnection():
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:scorpion.database.windows.net;DATABASE=db;UID=scorpion;PWD=Pa55w.rd1234')
    return cnxn


# Function for reading data from Azure SQL
def sqlread(qry):
    cnxn = sqlconnection()
    DF = pd.read_sql_query(qry, cnxn)  # Executing the Query
    cnxn.close()
    return DF


# Function for writing data to Azure SQL
def sqlwrite(qry):
    # Creating Cursor for the connection
    cnxn = sqlconnection()
    cur = cnxn.cursor()
    result = cur.execute(qry)  # Executing the Query
    cur.commit()
    cnxn.close()
    return result


# function to assign zero if no data exist for amountsDeposited and amountsWithdrawn
def emptyamountsDepositedamountsWithdrawnChecker(rawDf):
    # Verifying the data frame is not empty
    if (len(rawDf) == 0):

        amountsDeposited = 0
        amountsWithdrawn = 0
    else:
        # Logic to execute if the data is a panda series
        if (isinstance(rawDf['amountsDeposited'], pd.Series)):

            amountsDeposited = abs(rawDf['amountsDeposited'].values[0])
            amountsWithdrawn = abs(rawDf['amountsWithdrawn'].values[0])
        # Logic to execute if the data is not a panda series
        else:

            amountsDeposited = abs(rawDf['amountsDeposited'])
            amountsWithdrawn = abs(rawDf['amountsWithdrawn'])

    return amountsDeposited, amountsWithdrawn


# function For accumulating data based on money sent to accountdetails
def accountDetailsGrouper(fullTransactiondf):
    sixmonthsagoaccountDetailsgroupeddf = fullTransactiondf[
        ((pd.DatetimeIndex(fullTransactiondf['date']) > sixmonthsago))].groupby(['accountDetails']).agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})

    threemonthsagoaccountDetailsgroupeddf = fullTransactiondf[
        (pd.DatetimeIndex(fullTransactiondf['date']) > threemonthsago)].groupby(['accountDetails']).agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})

    return sixmonthsagoaccountDetailsgroupeddf, threemonthsagoaccountDetailsgroupeddf


# function For accumulating data based on individual account
def indAccntGrouper(indDf,accountFlagDf):
    # fetching data if the person is putting money frequenctly
    frequentDepositSum = indDf[indDf['transaction'] == 'Deposit-MB-Email Money Trf'].groupby(['year', 'month'])[
        'amountsDeposited'].sum().mean()

    # fetching six month old data for the individual account and summing it up
    TotalSixMonthAgoData = indDf[(pd.DatetimeIndex(indDf['date']) > sixmonthsago)][
        ['amountsDeposited', 'amountsWithdrawn']].agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})

    # fetching three month old data for the individual account and summing it up
    TotalThreeMonthAgoData = indDf[(pd.DatetimeIndex(indDf['date']) > threemonthsago)][
        ['amountsDeposited', 'amountsWithdrawn']].agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})
    # finding flag value for transit
    flaggedData = accountFlagDf[accountFlagDf['accountNumber'] == indDf['accountNumber'].unique()[0]]

    # Verifying the data frame is not null
    if (len(flaggedData) != 0):

        flaggedAccntIndVal = flaggedData['flag'].values[0]
    else:

        flaggedAccntIndVal = 0

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    sixmonthTotalamountsDeposited_MA, sixmonthTotalamountsWithdrawn_MA = emptyamountsDepositedamountsWithdrawnChecker(
        TotalSixMonthAgoData)

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    threemonthTotalamountsDeposited_MA, threemonthTotalamountsWithdrawn_MA = emptyamountsDepositedamountsWithdrawnChecker(
        TotalThreeMonthAgoData)

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    sixmonthTotalamountsDeposited_MA, sixmonthTotalamountsWithdrawn_MA = emptyamountsDepositedamountsWithdrawnChecker(
        TotalSixMonthAgoData)

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    threemonthTotalamountsDeposited_MA, threemonthTotalamountsWithdrawn_MA = emptyamountsDepositedamountsWithdrawnChecker(
        TotalThreeMonthAgoData)

    # calculating mean number of accounts to and from transactions happened
    meanTranscPerDisctAccntPerMonth = indDf.groupby(['year', 'month'])['accountDetails'].nunique().mean()

    # count Based on accountDetails
    countaccountdetailsdf = indDf.groupby(['accountDetails'])['transactionType'].count()
    return flaggedAccntIndVal,frequentDepositSum, sixmonthTotalamountsDeposited_MA, sixmonthTotalamountsWithdrawn_MA, threemonthTotalamountsDeposited_MA, threemonthTotalamountsWithdrawn_MA, meanTranscPerDisctAccntPerMonth, countaccountdetailsdf


# function For accumulating data based on money sent to accountdetails involving current transactions
def currentransacAccountDetailsGrouper(indDf, inddailyTransfrDf):
    # filtering only data whose other account is included in todays transaction
    accountDetailsFiltered = indDf[indDf.accountDetails.isin(inddailyTransfrDf.accountDetails.unique())]

    # fetching six month old data for the individual account and summing it up based on other account
    sixmonthsagodf = accountDetailsFiltered[
        (pd.DatetimeIndex(accountDetailsFiltered['date']) > sixmonthsago)].groupby(['accountDetails']).agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})
    # fetching three month old data for the individual account and summing it up based on other account
    threemonthsagodf = accountDetailsFiltered[
        (pd.DatetimeIndex(accountDetailsFiltered['date']) > threemonthsago)].groupby(['accountDetails']).agg(
        {'amountsDeposited': 'sum', 'amountsWithdrawn': 'sum'})

    return sixmonthsagodf, threemonthsagodf


# function for fetching data based on money sent to accountdetails per transactionbasis
def perTransactionAccountDetailsRule(sixmonthsagoaccountDetailsgroupeddf, threemonthsagoaccountDetailsgroupeddf, j,
                                     countaccountdetailsdf, sixmonthsagodf, threemonthsagodf,accountFlagDf):
    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    sixmonthsagoaccountDetailsgroupDepositeddata, sixmonthsagoaccountDetailsgroupWithdrawndata = emptyamountsDepositedamountsWithdrawnChecker(
        sixmonthsagoaccountDetailsgroupeddf[sixmonthsagoaccountDetailsgroupeddf.index.values == j])

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    threemonthsagoaccountDetailsgroupDepositeddata, threemonthsagoaccountDetailsgroupWithdrawndata = emptyamountsDepositedamountsWithdrawnChecker(
        threemonthsagoaccountDetailsgroupeddf[threemonthsagoaccountDetailsgroupeddf.index.values == j])

    # finding flag value for transit
    flaggedData = accountFlagDf[accountFlagDf['accountNumber'] == j]

    # Verifying the data frame is not null
    if (len(flaggedData) != 0):

        flaggedAccntVal = (countaccountdetailsdf[countaccountdetailsdf.index.values == j].values[0]) * (
            flaggedData['flag'].values[0])
    else:

        flaggedAccntVal = 0

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    sixmonthTotalamountsDeposited_OA, sixmonthTotalamountsWithdrawn_OA = emptyamountsDepositedamountsWithdrawnChecker(
        sixmonthsagodf[sixmonthsagodf.index.values == j])

    # calling function to return zero if data doesnt exist for amountsDeposited and amountsWithdrawn
    threemonthTotalamountsDeposited_OA, threemonthTotalamountsWithdrawn_OA = emptyamountsDepositedamountsWithdrawnChecker(
        threemonthsagodf[threemonthsagodf.index.values == j])

    return flaggedAccntVal, sixmonthsagoaccountDetailsgroupDepositeddata, sixmonthsagoaccountDetailsgroupWithdrawndata, threemonthsagoaccountDetailsgroupDepositeddata, threemonthsagoaccountDetailsgroupWithdrawndata, sixmonthTotalamountsDeposited_OA, sixmonthTotalamountsWithdrawn_OA, threemonthTotalamountsDeposited_OA, threemonthTotalamountsWithdrawn_OA


# function which modifies the individual data
def indvuser(indDf, inddailyTransfrDf, accountFlagDf, fullTransactiondf):
    flaggedTransacDetail = pd.DataFrame(
        columns=['accountNumber', 'meanTranscPerDisctAccntPerMonth', 'sixmonthTotalamountsDeposited_OA',
                 'sixmonthTotalamountsWithdrawn_OA',
                 'threemonthTotalamountsDeposited_OA', 'threemonthTotalamountsWithdrawn_OA',
                 'sixmonthTotalamountsDeposited_MA',
                 'sixmonthTotalamountsWithdrawn_MA', 'threemonthTotalamountsDeposited_MA',
                 'threemonthTotalamountsWithdrawn_MA',
                 'frequentDepositSum', 'sixmonthsagoaccountDetailsgroupDepositeddata',
                 'sixmonthsagoaccountDetailsgroupWithdrawndata',
                 'threemonthsagoaccountDetailsgroupDepositeddata', 'threemonthsagoaccountDetailsgroupWithdrawndata',
                 'flaggedAccntVal', 'accountDetails'])

    if (len(indDf) == 0):

        print("No Prev Data available...")

    else:

        # Calling Function to return overall accountdetails transaction aggregator
        sixmonthsagoaccountDetailsgroupeddf, threemonthsagoaccountDetailsgroupeddf = accountDetailsGrouper(
            fullTransactiondf)

        # Calling function For accumulating data based on individual account
        flaggedAccntIndVal,frequentDepositSum, sixmonthTotalamountsDeposited_MA, sixmonthTotalamountsWithdrawn_MA, threemonthTotalamountsDeposited_MA, threemonthTotalamountsWithdrawn_MA, meanTranscPerDisctAccntPerMonth, countaccountdetailsdf = indAccntGrouper(
            indDf,accountFlagDf)

        # Calling function For accumulating data based on money sent to accountdetails involving current transactions
        sixmonthsagodf, threemonthsagodf = currentransacAccountDetailsGrouper(indDf, inddailyTransfrDf)

        # looping through other account details
        for j in indDf.accountDetails.unique():
            # calling function for fetching data based on money sent to accountdetails per transactionbasis
            flaggedAccntDetVal, sixmonthsagoaccountDetailsgroupDepositeddata, sixmonthsagoaccountDetailsgroupWithdrawndata, threemonthsagoaccountDetailsgroupDepositeddata, threemonthsagoaccountDetailsgroupWithdrawndata, sixmonthTotalamountsDeposited_OA, sixmonthTotalamountsWithdrawn_OA, threemonthTotalamountsDeposited_OA, threemonthTotalamountsWithdrawn_OA = perTransactionAccountDetailsRule(
                sixmonthsagoaccountDetailsgroupeddf, threemonthsagoaccountDetailsgroupeddf, j, countaccountdetailsdf,
                sixmonthsagodf, threemonthsagodf,accountFlagDf)
            flaggedAccntVal = flaggedAccntDetVal + flaggedAccntIndVal
            # creating data frame for the required output and appending to parent one
            tempdf = pd.DataFrame(
                columns=['accountNumber', 'meanTranscPerDisctAccntPerMonth', 'sixmonthTotalamountsDeposited_OA',
                         'sixmonthTotalamountsWithdrawn_OA', 'threemonthTotalamountsDeposited_OA',
                         'threemonthTotalamountsWithdrawn_OA',
                         'sixmonthTotalamountsDeposited_MA', 'sixmonthTotalamountsWithdrawn_MA',
                         'threemonthTotalamountsDeposited_MA',
                         'threemonthTotalamountsWithdrawn_MA', 'frequentDepositSum',
                         'sixmonthsagoaccountDetailsgroupDepositeddata',
                         'sixmonthsagoaccountDetailsgroupWithdrawndata',
                         'threemonthsagoaccountDetailsgroupDepositeddata',
                         'threemonthsagoaccountDetailsgroupWithdrawndata', 'flaggedAccntVal', 'accountDetails'])

            tempdf = pd.DataFrame([[indDf['accountNumber'].unique()[0], meanTranscPerDisctAccntPerMonth,
                                    sixmonthTotalamountsDeposited_OA, sixmonthTotalamountsWithdrawn_OA,
                                    threemonthTotalamountsDeposited_OA,
                                    threemonthTotalamountsWithdrawn_OA, sixmonthTotalamountsDeposited_MA,
                                    sixmonthTotalamountsWithdrawn_MA,
                                    threemonthTotalamountsDeposited_MA, threemonthTotalamountsWithdrawn_MA,
                                    frequentDepositSum, sixmonthsagoaccountDetailsgroupDepositeddata,
                                    sixmonthsagoaccountDetailsgroupWithdrawndata,
                                    threemonthsagoaccountDetailsgroupDepositeddata,
                                    threemonthsagoaccountDetailsgroupWithdrawndata, flaggedAccntVal, j]],
                                  columns=['accountNumber', 'meanTranscPerDisctAccntPerMonth',
                                           'sixmonthTotalamountsDeposited_OA',
                                           'sixmonthTotalamountsWithdrawn_OA', 'threemonthTotalamountsDeposited_OA',
                                           'threemonthTotalamountsWithdrawn_OA',
                                           'sixmonthTotalamountsDeposited_MA', 'sixmonthTotalamountsWithdrawn_MA',
                                           'threemonthTotalamountsDeposited_MA',
                                           'threemonthTotalamountsWithdrawn_MA', 'frequentDepositSum',
                                           'sixmonthsagoaccountDetailsgroupDepositeddata',
                                           'sixmonthsagoaccountDetailsgroupWithdrawndata',
                                           'threemonthsagoaccountDetailsgroupDepositeddata',
                                           'threemonthsagoaccountDetailsgroupWithdrawndata', 'flaggedAccntVal',
                                           'accountDetails'])

            flaggedTransacDetail = flaggedTransacDetail.append(tempdf)

    return flaggedTransacDetail

# function to filter out dataframe based on the criteria specified by bank
def dataThreshholdFilter(new_df):
    new_df = new_df[(new_df['meanTranscPerDisctAccntPerMonth'] <= threshHold_meanTranscPerDisctAccntPerMonth) |
                    (new_df['sixmonthTotalamountsDeposited_OA'] >= threshHold_sixmonthTotalamountsDeposited_OA) |
                    (new_df['sixmonthTotalamountsWithdrawn_OA'] >= threshHold_sixmonthTotalamountsWithdrawn_OA) |
                    (new_df['threemonthTotalamountsDeposited_OA'] >= threshHold_threemonthTotalamountsDeposited_OA) |
                    (new_df['threemonthTotalamountsWithdrawn_OA'] >= threshHold_threemonthTotalamountsWithdrawn_OA) |
                    (new_df['sixmonthTotalamountsDeposited_MA'] >= threshHold_sixmonthTotalamountsDeposited_MA) |
                    (new_df['sixmonthTotalamountsWithdrawn_MA'] >= threshHold_sixmonthTotalamountsWithdrawn_MA) |
                    (new_df['threemonthTotalamountsDeposited_MA'] >= threshHold_threemonthTotalamountsDeposited_MA) |
                    (new_df['threemonthTotalamountsWithdrawn_MA'] >= threshHold_threemonthTotalamountsWithdrawn_MA) |
                    (new_df['frequentDepositSum'] >= threshHold_frequentDepositSum) |
                    (new_df['sixmonthsagoaccountDetailsgroupDepositeddata'] >= threshHold_sixmonthsagoaccountDetailsgroupDepositeddata) |
                    (new_df['sixmonthsagoaccountDetailsgroupWithdrawndata'] >= threshHold_sixmonthsagoaccountDetailsgroupWithdrawndata) |
                    (new_df['threemonthsagoaccountDetailsgroupDepositeddata'] >= threshHold_threemonthsagoaccountDetailsgroupDepositeddata) |
                    (new_df['threemonthsagoaccountDetailsgroupWithdrawndata'] >= threshHold_threemonthsagoaccountDetailsgroupWithdrawndata) |
                    (new_df['flaggedAccntVal'] >= threshHold_flaggedAccntVal)]

    return new_df

#function to rearrange the columns so as to be in the schema of trained ML model
def rearrangeForMl(disorderedDf):
    columnList =[
        'accountNumber',
        'institution',
        'transit',
        'date',
        'transaction',
        'transactionType',
        'openingBalance',
        'amountsWithdrawn',
        'amountsDeposited',
        'balance',
        'mlIdentifier',
        'meanTranscPerDisctAccntPerMonth',
        'sixmonthTotalamountsDeposited_OA',
        'sixmonthTotalamountsWithdrawn_OA',
        'threemonthTotalamountsDeposited_OA',
        'threemonthTotalamountsWithdrawn_OA',
        'sixmonthTotalamountsDeposited_MA',
        'sixmonthTotalamountsWithdrawn_MA',
        'threemonthTotalamountsDeposited_MA',
        'threemonthTotalamountsWithdrawn_MA',
        'frequentDepositSum',
        'sixmonthsagoaccountDetailsgroupDepositeddata',
        'sixmonthsagoaccountDetailsgroupWithdrawndata',
        'threemonthsagoaccountDetailsgroupDepositeddata',
        'threemonthsagoaccountDetailsgroupWithdrawndata',
        'flaggedAccntVal',
        'meanTranscPerDisctAccntPerMonth_Rank',
        'sixmonthTotalamountsDeposited_OA_Rank',
        'sixmonthTotalamountsWithdrawn_OA_Rank',
        'threemonthTotalamountsDeposited_OA_Rank',
        'threemonthTotalamountsWithdrawn_OA_Rank',
        'sixmonthTotalamountsDeposited_MA_Rank',
        'sixmonthTotalamountsWithdrawn_MA_Rank',
        'threemonthTotalamountsDeposited_MA_Rank',
        'threemonthTotalamountsWithdrawn_MA_Rank',
        'frequentDepositSum_Rank',
        'sixmonthsagoaccountDetailsgroupDepositeddata_Rank',
        'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank',
        'threemonthsagoaccountDetailsgroupDepositeddata_Rank',
        'threemonthsagoaccountDetailsgroupWithdrawndata_Rank',
        'flaggedAccntVal_Rank',
        'accountDetails']
    finaldf = pd.DataFrame(columns=columnList)

    for k, row in disorderedDf.iterrows():

        finaldf.loc[k] = (
                            row['accountNumber'],
                            row['institution'],
                            row['transit'],
                            row['date'],
                            row['transaction'],
                            row['transactionType'],
                            row['openingBalance'],
                            row['amountsWithdrawn'],
                            row['amountsDeposited'],
                            row['balance'],
                            row['mlIdentifier'],
                            row['meanTranscPerDisctAccntPerMonth'],
                            row['sixmonthTotalamountsDeposited_OA'],
                            row['sixmonthTotalamountsWithdrawn_OA'],
                            row['threemonthTotalamountsDeposited_OA'],
                            row['threemonthTotalamountsWithdrawn_OA'],
                            row['sixmonthTotalamountsDeposited_MA'],
                            row['sixmonthTotalamountsWithdrawn_MA'],
                            row['threemonthTotalamountsDeposited_MA'],
                            row['threemonthTotalamountsWithdrawn_MA'],
                            row['frequentDepositSum'],
                            row['sixmonthsagoaccountDetailsgroupDepositeddata'],
                            row['sixmonthsagoaccountDetailsgroupWithdrawndata'],
                            row['threemonthsagoaccountDetailsgroupDepositeddata'],
                            row['threemonthsagoaccountDetailsgroupWithdrawndata'],
                            row['flaggedAccntVal'],
                            row['meanTranscPerDisctAccntPerMonth_Rank'],
                            row['sixmonthTotalamountsDeposited_OA_Rank'],
                            row['sixmonthTotalamountsWithdrawn_OA_Rank'],
                            row['threemonthTotalamountsDeposited_OA_Rank'],
                            row['threemonthTotalamountsWithdrawn_OA_Rank'],
                            row['sixmonthTotalamountsDeposited_MA_Rank'],
                            row['sixmonthTotalamountsWithdrawn_MA_Rank'],
                            row['threemonthTotalamountsDeposited_MA_Rank'],
                            row['threemonthTotalamountsWithdrawn_MA_Rank'],
                            row['frequentDepositSum_Rank'],
                            row['sixmonthsagoaccountDetailsgroupDepositeddata_Rank'],
                            row['sixmonthsagoaccountDetailsgroupWithdrawndata_Rank'],
                            row['threemonthsagoaccountDetailsgroupDepositeddata_Rank'],
                            row['threemonthsagoaccountDetailsgroupWithdrawndata_Rank'],
                            row['flaggedAccntVal_Rank'],
                            row['accountDetails'])

    return finaldf

#function to assign ranking for flags
def ranking(new_df):
    new_df['meanTranscPerDisctAccntPerMonth_Rank'] = threshHold_meanTranscPerDisctAccntPerMonth - new_df[
        'meanTranscPerDisctAccntPerMonth']
    new_df['sixmonthTotalamountsDeposited_OA_Rank'] = new_df[
                                                          'sixmonthTotalamountsDeposited_OA'] - threshHold_sixmonthTotalamountsDeposited_OA
    new_df['sixmonthTotalamountsWithdrawn_OA_Rank'] = new_df[
                                                          'sixmonthTotalamountsWithdrawn_OA'] - threshHold_sixmonthTotalamountsWithdrawn_OA
    new_df['threemonthTotalamountsDeposited_OA_Rank'] = new_df[
                                                            'threemonthTotalamountsDeposited_OA'] - threshHold_threemonthTotalamountsDeposited_OA
    new_df['threemonthTotalamountsWithdrawn_OA_Rank'] = new_df[
                                                            'threemonthTotalamountsWithdrawn_OA'] - threshHold_threemonthTotalamountsWithdrawn_OA
    new_df['sixmonthTotalamountsDeposited_MA_Rank'] = new_df[
                                                          'sixmonthTotalamountsDeposited_MA'] - threshHold_sixmonthTotalamountsDeposited_MA
    new_df['sixmonthTotalamountsWithdrawn_MA_Rank'] = new_df[
                                                          'sixmonthTotalamountsWithdrawn_MA'] - threshHold_sixmonthTotalamountsWithdrawn_MA
    new_df['threemonthTotalamountsDeposited_MA_Rank'] = new_df[
                                                            'threemonthTotalamountsDeposited_MA'] - threshHold_threemonthTotalamountsDeposited_MA
    new_df['threemonthTotalamountsWithdrawn_MA_Rank'] = new_df[
                                                            'threemonthTotalamountsWithdrawn_MA'] - threshHold_threemonthTotalamountsWithdrawn_MA
    new_df['frequentDepositSum_Rank'] = new_df['frequentDepositSum'] - threshHold_frequentDepositSum
    new_df['sixmonthsagoaccountDetailsgroupDepositeddata_Rank'] = new_df[
                                                                      'sixmonthsagoaccountDetailsgroupDepositeddata'] - threshHold_sixmonthsagoaccountDetailsgroupDepositeddata
    new_df['sixmonthsagoaccountDetailsgroupWithdrawndata_Rank'] = new_df[
                                                                      'sixmonthsagoaccountDetailsgroupWithdrawndata'] - threshHold_sixmonthsagoaccountDetailsgroupWithdrawndata
    new_df['threemonthsagoaccountDetailsgroupDepositeddata_Rank'] = new_df[
                                                                        'threemonthsagoaccountDetailsgroupDepositeddata'] - threshHold_threemonthsagoaccountDetailsgroupDepositeddata
    new_df['threemonthsagoaccountDetailsgroupWithdrawndata_Rank'] = new_df[
                                                                        'threemonthsagoaccountDetailsgroupWithdrawndata'] - threshHold_threemonthsagoaccountDetailsgroupWithdrawndata
    new_df['flaggedAccntVal_Rank'] = new_df['flaggedAccntVal'] - threshHold_flaggedAccntVal

    new_df['meanTranscPerDisctAccntPerMonth_Rank'] = (new_df['meanTranscPerDisctAccntPerMonth_Rank'].max() - new_df[
        'meanTranscPerDisctAccntPerMonth_Rank']) / (new_df['meanTranscPerDisctAccntPerMonth_Rank'].max() or 1)
    new_df['sixmonthTotalamountsDeposited_OA_Rank'] = (new_df['sixmonthTotalamountsDeposited_OA_Rank'].max() - new_df[
        'sixmonthTotalamountsDeposited_OA_Rank']) / (new_df['sixmonthTotalamountsDeposited_OA_Rank'].max() or 1)
    new_df['sixmonthTotalamountsWithdrawn_OA_Rank'] = (new_df['sixmonthTotalamountsWithdrawn_OA_Rank'].max() - new_df[
        'sixmonthTotalamountsWithdrawn_OA_Rank']) / (new_df['sixmonthTotalamountsWithdrawn_OA_Rank'].max() or 1)
    new_df['threemonthTotalamountsDeposited_OA_Rank'] = (new_df['threemonthTotalamountsDeposited_OA_Rank'].max() -
                                                         new_df['threemonthTotalamountsDeposited_OA_Rank']) / (new_df[
                                                                                                                   'threemonthTotalamountsDeposited_OA_Rank'].max() or 1)
    new_df['threemonthTotalamountsWithdrawn_OA_Rank'] = (new_df['threemonthTotalamountsWithdrawn_OA_Rank'].max() -
                                                         new_df['threemonthTotalamountsWithdrawn_OA_Rank']) / (new_df[
                                                                                                                   'threemonthTotalamountsWithdrawn_OA_Rank'].max() or 1)
    new_df['sixmonthTotalamountsDeposited_MA_Rank'] = (new_df['sixmonthTotalamountsDeposited_MA_Rank'].max() - new_df[
        'sixmonthTotalamountsDeposited_MA_Rank']) / (new_df['sixmonthTotalamountsDeposited_MA_Rank'].max() or 1)
    new_df['sixmonthTotalamountsWithdrawn_MA_Rank'] = (new_df['sixmonthTotalamountsWithdrawn_MA_Rank'].max() - new_df[
        'sixmonthTotalamountsWithdrawn_MA_Rank']) / (new_df['sixmonthTotalamountsWithdrawn_MA_Rank'].max() or 1)
    new_df['threemonthTotalamountsDeposited_MA_Rank'] = (new_df['threemonthTotalamountsDeposited_MA_Rank'].max() -
                                                         new_df['threemonthTotalamountsDeposited_MA_Rank']) / (new_df[
                                                                                                                   'threemonthTotalamountsDeposited_MA_Rank'].max() or 1)
    new_df['threemonthTotalamountsWithdrawn_MA_Rank'] = (new_df['threemonthTotalamountsWithdrawn_MA_Rank'].max() -
                                                         new_df['threemonthTotalamountsWithdrawn_MA_Rank']) / (new_df[
                                                                                                                   'threemonthTotalamountsWithdrawn_MA_Rank'].max() or 1)
    new_df['frequentDepositSum_Rank'] = (new_df['frequentDepositSum_Rank'].max() - new_df[
        'frequentDepositSum_Rank']) / (new_df['frequentDepositSum_Rank'].max() or 1)
    new_df['sixmonthsagoaccountDetailsgroupDepositeddata_Rank'] = (new_df[
                                                                       'sixmonthsagoaccountDetailsgroupDepositeddata_Rank'].max() -
                                                                   new_df[
                                                                       'sixmonthsagoaccountDetailsgroupDepositeddata_Rank']) / (
                                                                              new_df[
                                                                                  'sixmonthsagoaccountDetailsgroupDepositeddata_Rank'].max() or 1)
    new_df['sixmonthsagoaccountDetailsgroupWithdrawndata_Rank'] = (new_df[
                                                                       'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank'].max() -
                                                                   new_df[
                                                                       'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank']) / (
                                                                              new_df[
                                                                                  'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank'].max() or 1)
    new_df['threemonthsagoaccountDetailsgroupDepositeddata_Rank'] = (new_df[
                                                                         'threemonthsagoaccountDetailsgroupDepositeddata_Rank'].max() -
                                                                     new_df[
                                                                         'threemonthsagoaccountDetailsgroupDepositeddata_Rank']) / (
                                                                                new_df[
                                                                                    'threemonthsagoaccountDetailsgroupDepositeddata_Rank'].max() or 1)
    new_df['threemonthsagoaccountDetailsgroupWithdrawndata_Rank'] = (new_df[
                                                                         'threemonthsagoaccountDetailsgroupWithdrawndata_Rank'].max() -
                                                                     new_df[
                                                                         'threemonthsagoaccountDetailsgroupWithdrawndata_Rank']) / (
                                                                                new_df[
                                                                                    'threemonthsagoaccountDetailsgroupWithdrawndata_Rank'].max() or 1)
    new_df['flaggedAccntVal_Rank'] = (new_df['flaggedAccntVal_Rank'].max() - new_df['flaggedAccntVal_Rank']) / (
                new_df['flaggedAccntVal_Rank'].max() or 1)

    new_df[new_df[['meanTranscPerDisctAccntPerMonth_Rank', 'sixmonthTotalamountsDeposited_OA_Rank',
                   'sixmonthTotalamountsWithdrawn_OA_Rank',
                   'threemonthTotalamountsDeposited_OA_Rank', 'threemonthTotalamountsWithdrawn_OA_Rank',
                   'sixmonthTotalamountsDeposited_MA_Rank', 'sixmonthTotalamountsWithdrawn_MA_Rank',
                   'threemonthTotalamountsDeposited_MA_Rank',
                   'threemonthTotalamountsWithdrawn_MA_Rank', 'frequentDepositSum_Rank',
                   'sixmonthsagoaccountDetailsgroupDepositeddata_Rank',
                   'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank',
                   'threemonthsagoaccountDetailsgroupDepositeddata_Rank',
                   'threemonthsagoaccountDetailsgroupWithdrawndata_Rank', 'flaggedAccntVal_Rank']] < 0] = float("NAN")

    new_df['rank'] = (new_df[['meanTranscPerDisctAccntPerMonth_Rank', 'sixmonthTotalamountsDeposited_OA_Rank',
                              'sixmonthTotalamountsWithdrawn_OA_Rank',
                              'threemonthTotalamountsDeposited_OA_Rank', 'threemonthTotalamountsWithdrawn_OA_Rank',
                              'sixmonthTotalamountsDeposited_MA_Rank', 'sixmonthTotalamountsWithdrawn_MA_Rank',
                              'threemonthTotalamountsDeposited_MA_Rank',
                              'threemonthTotalamountsWithdrawn_MA_Rank', 'frequentDepositSum_Rank',
                              'sixmonthsagoaccountDetailsgroupDepositeddata_Rank',
                              'sixmonthsagoaccountDetailsgroupWithdrawndata_Rank',
                              'threemonthsagoaccountDetailsgroupDepositeddata_Rank',
                              'threemonthsagoaccountDetailsgroupWithdrawndata_Rank', 'flaggedAccntVal_Rank']].mean(
        axis=1)*10).astype(int)+ 1
    new_df.fillna(0, inplace=True)

    #new_df['rank'] = new_df['rank'] - new_df['rank'].min() + 1

    return new_df

#function to predict ML Value
def mlPredict(elem):
    data = {"data":[elem]}
    # Convert to JSON string.
    input_data = json.dumps(data)

    # Set the content type.
    headers = {'Content-Type': 'application/json'}

    # Make the request and display the response.
    resp = requests.post(scoring_uri, input_data, headers=headers)
    return resp

qry = 'SELECT * FROM dbo.transactions'
datatest = sqlread(qry)
fullTransactiondf = pd.DataFrame(data=datatest)

# Forming Day , month and year column
fullTransactiondf['day'] = pd.DatetimeIndex(fullTransactiondf['date']).day
fullTransactiondf['month'] = pd.DatetimeIndex(fullTransactiondf['date']).month
fullTransactiondf['year'] = pd.DatetimeIndex(fullTransactiondf['date']).year

# Changing data type to flot for amount columns
fullTransactiondf['amountsDeposited'] = fullTransactiondf['amountsDeposited'].astype(float)
fullTransactiondf['amountsWithdrawn'] = fullTransactiondf['amountsWithdrawn'].astype(float)
fullTransactiondf['amountsDeposited'] = fullTransactiondf['amountsDeposited'].astype(float)
fullTransactiondf['amountsWithdrawn'] = fullTransactiondf['amountsWithdrawn'].astype(float)
dailytransdf = fullTransactiondf[fullTransactiondf['date'] == '2020-09-20']

#2020-09-16,2020-09-17,2020-09-18,2020-09-19,2020-09-20,2020-09-21,2020-09-22,2020-09-23,2020-09-24,2020-09-25
#dailytransdf = fullTransactiondf

qry = 'SELECT * FROM dbo.accountflag'
accountflagdata = sqlread(qry)
accountFlagDf = pd.DataFrame(data=accountflagdata)
print(f"Total Transactions in History :{ len(fullTransactiondf) }")

print(f"Total Daily Transaction :{ len(dailytransdf) }")

# Looping through daily transac based on unique account Numbers
for i in dailytransdf['accountNumber'].unique():
    specificaccount = fullTransactiondf[fullTransactiondf.accountNumber == i]
    tempDf = indvuser(specificaccount, dailytransdf[dailytransdf.accountNumber == i], accountFlagDf, fullTransactiondf)
    tempDf = tempDf.fillna(0)
    ModifiedIndDf = ModifiedIndDf.append(tempDf)

# merging the transaction dataframe and formed flags data frame
new_df = pd.merge(dailytransdf, ModifiedIndDf, how='left', left_on=['accountNumber', 'accountDetails'],
                  right_on=['accountNumber', 'accountDetails'])
print(f"Merged Transaction Details Count : { len(new_df) }")
if (len(new_df)!=0):
    casesdf = ranking(new_df)
    for k, row in new_df.iterrows():
        qry = f"INSERT INTO newfilteredrow ([rank], [accountNumber], [accountDetails], [institution], [transit], [date], [transaction], [transactionType], [openingBalance], [amountsWithdrawn], [amountsDeposited], [balance], [meanTranscPerDisctAccntPerMonth], [sixmonthTotalamountsDeposited_OA], [sixmonthTotalamountsWithdrawn_OA], [threemonthTotalamountsDeposited_OA], [threemonthTotalamountsWithdrawn_OA], [sixmonthTotalamountsDeposited_MA], [sixmonthTotalamountsWithdrawn_MA], [threemonthTotalamountsDeposited_MA], [threemonthTotalamountsWithdrawn_MA], [frequentDepositSum], [sixmonthsagoaccountDetailsgroupDepositeddata], [sixmonthsagoaccountDetailsgroupWithdrawndata], [threemonthsagoaccountDetailsgroupDepositeddata], [threemonthsagoaccountDetailsgroupWithdrawndata], [flaggedAccntVal], [meanTranscPerDisctAccntPerMonth_Rank],[sixmonthTotalamountsDeposited_OA_Rank],[sixmonthTotalamountsWithdrawn_OA_Rank],[threemonthTotalamountsDeposited_OA_Rank],[threemonthTotalamountsWithdrawn_OA_Rank],[sixmonthTotalamountsDeposited_MA_Rank],[sixmonthTotalamountsWithdrawn_MA_Rank],[threemonthTotalamountsDeposited_MA_Rank],[threemonthTotalamountsWithdrawn_MA_Rank],[frequentDepositSum_Rank],[sixmonthsagoaccountDetailsgroupDepositeddata_Rank],[sixmonthsagoaccountDetailsgroupWithdrawndata_Rank],[threemonthsagoaccountDetailsgroupDepositeddata_Rank],[threemonthsagoaccountDetailsgroupWithdrawndata_Rank],[flaggedAccntVal_Rank]) VALUES('{row['rank']}', '{row['accountNumber']}', '{row['accountDetails']}', '{row['institution']}', '{row['transit']}', '{row['date']}', '{row['transaction']}', '{row['transactionType']}', '{row['openingBalance']}', '{row['amountsWithdrawn']}', '{row['amountsDeposited']}', '{row['balance']}', '{row['meanTranscPerDisctAccntPerMonth']}', '{row['sixmonthTotalamountsDeposited_OA']}', '{row['sixmonthTotalamountsWithdrawn_OA']}', '{row['threemonthTotalamountsDeposited_OA']}', '{row['threemonthTotalamountsWithdrawn_OA']}', '{row['sixmonthTotalamountsDeposited_MA']}', '{row['sixmonthTotalamountsWithdrawn_MA']}', '{row['threemonthTotalamountsDeposited_MA']}', '{row['threemonthTotalamountsWithdrawn_MA']}', '{row['frequentDepositSum']}', '{row['sixmonthsagoaccountDetailsgroupDepositeddata']}','{row['sixmonthsagoaccountDetailsgroupWithdrawndata']}', '{row['threemonthsagoaccountDetailsgroupDepositeddata']}', '{row['threemonthsagoaccountDetailsgroupWithdrawndata']}', '{row['flaggedAccntVal']}', '{row['meanTranscPerDisctAccntPerMonth_Rank']}','{row['sixmonthTotalamountsDeposited_OA_Rank']}','{row['sixmonthTotalamountsWithdrawn_OA_Rank']}','{row['threemonthTotalamountsDeposited_OA_Rank']}','{row['threemonthTotalamountsWithdrawn_OA_Rank']}','{row['sixmonthTotalamountsDeposited_MA_Rank']}','{row['sixmonthTotalamountsWithdrawn_MA_Rank']}','{row['threemonthTotalamountsDeposited_MA_Rank']}','{row['threemonthTotalamountsWithdrawn_MA_Rank']}','{row['frequentDepositSum_Rank']}','{row['sixmonthsagoaccountDetailsgroupDepositeddata_Rank']}','{row['sixmonthsagoaccountDetailsgroupWithdrawndata_Rank']}','{row['threemonthsagoaccountDetailsgroupDepositeddata_Rank']}','{row['threemonthsagoaccountDetailsgroupWithdrawndata_Rank']}','{row['flaggedAccntVal_Rank']}')"
        print(f"Inserting Unfiltered Data for {row['accountNumber']} and other account {row['accountDetails']}")
        sqlwrite(qry)

    #calling function to remove data which dont meet the threshhold values
    casesdf = dataThreshholdFilter(casesdf)

    casesdf.drop(['rank'], axis='columns', inplace=True)

    #function call to rearrange the columns
    casesdf = rearrangeForMl(casesdf)

    #converting to list
    dataarr = np.array(casesdf).tolist()


    if (len(casesdf)==0):
        print("No new Money Laundering Cases Found Today...")
    else:
        casesdf['rank'] = 0
        casesdf['Action'] = 'Not Verified'
        for elemi in range(len(dataarr)):

            #Applying ML model
            resp = mlPredict(dataarr[elemi])
            mlRank =json.loads(resp.json())['result'][0]
            casesdf['rank'].loc[elemi] = int(mlRank)

        for k, row in casesdf.iterrows():
            qry = f"INSERT INTO newmlData ([rank], [accountNumber], [accountDetails], [institution], [transit], [date], [transaction], [transactionType], [openingBalance], [amountsWithdrawn], [amountsDeposited], [balance], [meanTranscPerDisctAccntPerMonth], [sixmonthTotalamountsDeposited_OA], [sixmonthTotalamountsWithdrawn_OA], [threemonthTotalamountsDeposited_OA], [threemonthTotalamountsWithdrawn_OA], [sixmonthTotalamountsDeposited_MA], [sixmonthTotalamountsWithdrawn_MA], [threemonthTotalamountsDeposited_MA], [threemonthTotalamountsWithdrawn_MA], [frequentDepositSum], [sixmonthsagoaccountDetailsgroupDepositeddata], [sixmonthsagoaccountDetailsgroupWithdrawndata], [threemonthsagoaccountDetailsgroupDepositeddata], [threemonthsagoaccountDetailsgroupWithdrawndata], [flaggedAccntVal], [meanTranscPerDisctAccntPerMonth_Rank],[sixmonthTotalamountsDeposited_OA_Rank],[sixmonthTotalamountsWithdrawn_OA_Rank],[threemonthTotalamountsDeposited_OA_Rank],[threemonthTotalamountsWithdrawn_OA_Rank],[sixmonthTotalamountsDeposited_MA_Rank],[sixmonthTotalamountsWithdrawn_MA_Rank],[threemonthTotalamountsDeposited_MA_Rank],[threemonthTotalamountsWithdrawn_MA_Rank],[frequentDepositSum_Rank],[sixmonthsagoaccountDetailsgroupDepositeddata_Rank],[sixmonthsagoaccountDetailsgroupWithdrawndata_Rank],[threemonthsagoaccountDetailsgroupDepositeddata_Rank],[threemonthsagoaccountDetailsgroupWithdrawndata_Rank],[flaggedAccntVal_Rank],[Action]) VALUES('{row['rank']}', '{row['accountNumber']}', '{row['accountDetails']}', '{row['institution']}', '{row['transit']}', '{row['date']}', '{row['transaction']}', '{row['transactionType']}', '{row['openingBalance']}', '{row['amountsWithdrawn']}', '{row['amountsDeposited']}', '{row['balance']}', '{row['meanTranscPerDisctAccntPerMonth']}', '{row['sixmonthTotalamountsDeposited_OA']}', '{row['sixmonthTotalamountsWithdrawn_OA']}', '{row['threemonthTotalamountsDeposited_OA']}', '{row['threemonthTotalamountsWithdrawn_OA']}', '{row['sixmonthTotalamountsDeposited_MA']}', '{row['sixmonthTotalamountsWithdrawn_MA']}', '{row['threemonthTotalamountsDeposited_MA']}', '{row['threemonthTotalamountsWithdrawn_MA']}', '{row['frequentDepositSum']}', '{row['sixmonthsagoaccountDetailsgroupDepositeddata']}','{row['sixmonthsagoaccountDetailsgroupWithdrawndata']}', '{row['threemonthsagoaccountDetailsgroupDepositeddata']}', '{row['threemonthsagoaccountDetailsgroupWithdrawndata']}', '{row['flaggedAccntVal']}', '{row['meanTranscPerDisctAccntPerMonth_Rank']}','{row['sixmonthTotalamountsDeposited_OA_Rank']}','{row['sixmonthTotalamountsWithdrawn_OA_Rank']}','{row['threemonthTotalamountsDeposited_OA_Rank']}','{row['threemonthTotalamountsWithdrawn_OA_Rank']}','{row['sixmonthTotalamountsDeposited_MA_Rank']}','{row['sixmonthTotalamountsWithdrawn_MA_Rank']}','{row['threemonthTotalamountsDeposited_MA_Rank']}','{row['threemonthTotalamountsWithdrawn_MA_Rank']}','{row['frequentDepositSum_Rank']}','{row['sixmonthsagoaccountDetailsgroupDepositeddata_Rank']}','{row['sixmonthsagoaccountDetailsgroupWithdrawndata_Rank']}','{row['threemonthsagoaccountDetailsgroupDepositeddata_Rank']}','{row['threemonthsagoaccountDetailsgroupWithdrawndata_Rank']}','{row['flaggedAccntVal_Rank']}','{row['Action']}')"
            print(f"Inserting Data for {row['accountNumber']} and other account {row['accountDetails']} classified as rank { row['rank'] }")
            sqlwrite(qry)
else:
    print("No Transactions Happened Today")
