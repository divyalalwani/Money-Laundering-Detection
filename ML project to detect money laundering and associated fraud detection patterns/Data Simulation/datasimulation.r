start.time <- Sys.time()
cat(paste('Started execution at', Sys.time(), '\n'))

# Set the working directory
setwd('C:/Users/wsadmin/Documents')


# pPevent output to CSV in scientific notation
options(scipen=999)

(library(dplyr))
(library(parallel))
(library(stringi))
library(profvis)
library(compiler)
library(data.table)
library(parallel)
library(Rcpp)
library(RODBC)
library(DBI)


# Set global constants

dateEnd <- as.Date('2020-09-30')
dateOrigin <- as.Date('2020-04-01')


#******* Functions *******

simulate.transaction.details <- function(account, start.date = dateOrigin, end.date = dateEnd, N = 20) {

   ACT.TRANSACTIONS <-ACCOUNTS[accountNumber == account]


   # replicate the rows of the data table N times
   ACT.TRANSACTIONS <- do.call("rbind", replicate(N, ACT.TRANSACTIONS, simplify = FALSE))
   #ACT.TRANSACTIONS[, referenceNumber := .I]
   ACT.TRANSACTIONS[, date := sample(seq(start.date, end.date, by="day"), N, replace = TRUE)]
   ACT.TRANSACTIONS[, transaction := sample(transactionTypes, N, replace = TRUE)]

   ACT.TRANSACTIONS[transaction %in% c("MB-Transfer to account", "Withdrawal-MB-Email Money Trf", "Point of Sale Purchase","Deposit-MB-Email Money Trf","MB-Transfer from account"
                                      , "Payroll dep.", "GST"), accountDetails := sample(money.transfer.accounts, .N, replace = TRUE)]
   ACT.TRANSACTIONS[transaction == "Fees/Dues", accountDetails := sample(fees, .N, replace = TRUE)]
   ACT.TRANSACTIONS[transaction == "Point of Sale Purchase", accountDetails := sample(pos, .N, replace = TRUE)]

   ACT.TRANSACTIONS <<- ACT.TRANSACTIONS[transaction %in% dTransactionTypes , transactionType := "Deposit"]
   ACT.TRANSACTIONS <<- ACT.TRANSACTIONS[transaction %in% wTransactionTypes , transactionType := "Withdrawal"]
   ACT.TRANSACTIONS[, openingBalance := rnorm(1, mean=95000, sd=10000)]

   ACT.TRANSACTIONS[transactionType == "Withdrawal", amountsWithdrawn := -1*(runif(.N, min=100, max= 6000))]
   ACT.TRANSACTIONS[transactionType == "Deposit", amountsWithdrawn := 0]
   ACT.TRANSACTIONS[transactionType == "Deposit", amountsDeposited := runif(.N, min=100, max= 8000)]
   ACT.TRANSACTIONS[transactionType == "Withdrawal", amountsDeposited := 0]
   ACT.TRANSACTIONS[transactionType == "Withdrawal", x := amountsWithdrawn]
   ACT.TRANSACTIONS[transactionType == "Deposit", x := amountsDeposited]
   ACT.TRANSACTIONS = ACT.TRANSACTIONS[order(date)]
   ACT.TRANSACTIONS[, balance := openingBalance + cumsum(x)]
   ACT.TRANSACTIONS[, mlIdentifier := FALSE]

   ACT.TRANSACTIONS <- ACT.TRANSACTIONS[, c("accountNumber", "institution", "transit", "date", "transaction", "accountDetails", "transactionType", "openingBalance",
                                            "amountsWithdrawn", "amountsDeposited", "balance", "mlIdentifier")]
   # Join rows to exsisting OPTIONS data table
   TRANSACTIONS <<- rbind(TRANSACTIONS, ACT.TRANSACTIONS, fill = TRUE)

}

simulate.transactions <- function(accounts, start.date, end.date) {


   for (i in accounts) {

      accounts.input <- i

      tryCatch(
         expr = {
            print(paste0('Start Time for: ', i, ': ', Sys.time()))
            simulate.transaction.details(account = accounts.input, start.date = dateOrigin, end.date = dateEnd, N = 100)
            print(paste0('End Time for: ', i, ': ', Sys.time()))
            cat('\n')
         },
         error = function(e) {

            message('Error occured for: ', i)
            message(e)
         }
      )
   }

}

ml.rule <- function(account.number, transactions, start.date, end.date, N, n, W, transaction, transaction.type, amounts.withdrawn, amounts.deposited, rule, account.details = NULL) {

   accountNumber <- account.number
   openingBalance <- rnorm(1, mean=100000, sd=3000)
   ACT.TRANSACTIONS <- data.table()
   ACT.TRANSACTIONS[, accountNumber := accountNumber]
   ACT.TRANSACTIONS[, institution := sample(institution, .N)]
   ACT.TRANSACTIONS[, transit := sample(transit, .N)]

   # replicate the rows of the data table N times
   ACT.TRANSACTIONS <- do.call("rbind", replicate(N, ACT.TRANSACTIONS, simplify = FALSE))
   #ACT.TRANSACTIONS[, referenceNumber := .I]
   ACT.TRANSACTIONS[, date := sample(seq(start.date, end.date, by="day"), N, replace = TRUE)]
   ACT.TRANSACTIONS[, transaction := sample(transactionTypes, N, replace = TRUE)]

   ACT.TRANSACTIONS[transaction %in% c("MB-Transfer to account", "Withdrawal-MB-Email Money Trf", "Point of Sale Purchase","Deposit-MB-Email Money Trf","MB-Transfer from account"
                                       , "Payroll dep.", "GST"), accountDetails := sample(money.transfer.accounts, .N, replace = TRUE)]
   ACT.TRANSACTIONS[transaction == "Fees/Dues", accountDetails := sample(fees, .N, replace = TRUE)]
   ACT.TRANSACTIONS[transaction == "Point of Sale Purchase", accountDetails := sample(pos, .N, replace = TRUE)]

   ACT.TRANSACTIONS <<- ACT.TRANSACTIONS[transaction %in% dTransactionTypes , transactionType := "Deposit"]
   ACT.TRANSACTIONS <<- ACT.TRANSACTIONS[transaction %in% wTransactionTypes , transactionType := "Withdrawal"]
   ACT.TRANSACTIONS[, openingBalance := openingBalance]
   ACT.TRANSACTIONS[transactionType == "Withdrawal", amountsWithdrawn := -1*(runif(.N, min=100, max= (openingBalance/100)))]
   ACT.TRANSACTIONS[transactionType == "Deposit", amountsDeposited := runif(.N, min=100, max= 8000)]
   ACT.TRANSACTIONS[, mlIdentifier := FALSE]

   RULE.1 <- data.table()
   RULE.1[, accountNumber := accountNumber]
   RULE.1[, institution := sample(institution, .N)]
   RULE.1[, transit := sample(transit, .N)]
   RULE.1 <- do.call("rbind", replicate(n,  RULE.1, simplify = FALSE))
   RULE.1[, date := start.date + W*.I]
   RULE.1[, transaction := transaction]


   if (rule == 1) {
      RULE.1[, accountDetails := sample(ml.accounts, .N, replace = TRUE)]
   } else  {
      RULE.1[, accountDetails := account.details]
   }

   RULE.1[, openingBalance := openingBalance]
   RULE.1[, transactionType := transaction.type]
   RULE.1[, amountsWithdrawn := -1*amounts.withdrawn]
   RULE.1[, amountsDeposited := amounts.deposited]
   RULE.1[, mlIdentifier := TRUE]

   ACT.TRANSACTIONS <- rbind(ACT.TRANSACTIONS, RULE.1, fill = TRUE)
   ACT.TRANSACTIONS[transactionType == "Deposit", amountsWithdrawn := 0]
   ACT.TRANSACTIONS[transactionType == "Withdrawal", amountsDeposited := 0]
   ACT.TRANSACTIONS[transactionType == "Withdrawal", x := amountsWithdrawn]
   ACT.TRANSACTIONS[transactionType == "Deposit", x := amountsDeposited]
   ACT.TRANSACTIONS = ACT.TRANSACTIONS[order(date)]
   ACT.TRANSACTIONS[, balance := openingBalance + cumsum(x)]

   ACT.TRANSACTIONS <- ACT.TRANSACTIONS[, c("accountNumber", "institution", "transit", "date", "transaction", "accountDetails", "transactionType", "openingBalance",
                                            "amountsWithdrawn", "amountsDeposited", "balance", "mlIdentifier")]
   # Join rows to exsisting OPTIONS data table
   TRANSACTIONS <<- rbind(TRANSACTIONS, ACT.TRANSACTIONS, fill = TRUE)

}



# #******* Data Simulator *******

ACCOUNTS <<- data.table()
accounts.full <<- paste0(runif(121, 1000000, 1999999) %/% 1)
accounts <<- accounts.full[1:100]
institution <<- c("002","003","004","006","010","016","030","039","117","127","177","219","245","260","269","270","308","309","310",
                  "315","320","338","340","509","540","608","614","623","809","815","819","828","829","837","839","865","879","889","899")
transit <<- paste0(runif(6300, 10000, 99999) %/% 1)
transactionTypes <<- c("MB-Transfer to account", "Withdrawal-MB-Email Money Trf",
                       "Fees/Dues", "Point of Sale Purchase","Deposit-MB-Email Money Trf","MB-Transfer from account"
                       , "Payroll dep.", "GST")
wTransactionTypes <<- c("MB-Transfer to account", "Withdrawal-MB-Email Money Trf", "Fees/Dues",
                        "Point of Sale Purchase")
dTransactionTypes <<- c("Deposit-MB-Email Money Trf","MB-Transfer from account", "Payroll dep.", "GST")

money.transfer.accounts <<- paste0(runif(20, 2000000, 2999999) %/% 1)
fees <<- paste0(runif(10, 3000000, 3999999) %/% 1)
pos <<- paste0(runif(10, 3000000, 3999999) %/% 1)
ml.accounts <<- paste0(runif(6, 4000000, 4999999) %/% 1)


ACCOUNTS <<- data.table()
ACCOUNTS <<- ACCOUNTS[, accountNumber := accounts]
ACCOUNTS[, institution := sample(institution, .N, replace = TRUE)]
ACCOUNTS[, transit := sample(transit, .N, replace = TRUE)]
TRANSACTIONS <<- data.table()

# Generate transaction details
cat('Started simulation for all accounts at:', as.character(Sys.time()), '\n')
simulate.transactions(accounts, dateOrigin, dateEnd)
cat('Finished simulation for all accounts at:', as.character(Sys.time()), '\n')



# Generate money laundering data
ml.rule(accounts.full[101], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer from account","Deposit", 0, 3000, 1)
ml.rule(accounts.full[102], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "Deposit-MB-Email Money Trf","Deposit", 0, 5000 ,1)
ml.rule(accounts.full[103], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "Payroll dep","Deposit", 0, 5000, 1)



ml.rule(accounts.full[104], TRANSACTIONS, dateOrigin, dateEnd, 100, 24, 7, "MB-Transfer to account","Withdrawal", 3500, 0, 1)
ml.rule(accounts.full[105], TRANSACTIONS, dateOrigin, dateEnd, 100, 24, 7, "MB-Transfer to account","Withdrawal", 3000, 0, 1)
ml.rule(accounts.full[106], TRANSACTIONS, dateOrigin, dateEnd, 100, 6, 14, "Insurance","Withdrawal", 700, 0, 1)


ml.rule(accounts.full[107], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 1)
ml.rule(accounts.full[108], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 1)
ml.rule(accounts.full[109], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 1)
ml.rule(accounts.full[110], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 1)
ml.rule(accounts.full[111], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 1)



ml.rule(accounts.full[112], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 3, 5302674)
ml.rule(accounts.full[113], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 3, 5302674)
ml.rule(accounts.full[114], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 3, 5302674)
ml.rule(accounts.full[115], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 3, 5302674)
ml.rule(accounts.full[116], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 2000, 0, 3, 5302674)

ml.rule(accounts.full[117], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 1999, 0, 3, 5566022)
ml.rule(accounts.full[118], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 1999, 0, 3, 5566022)
ml.rule(accounts.full[119], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 1999, 0, 3, 5566022)
ml.rule(accounts.full[120], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 1999, 0, 3, 5566022)
ml.rule(accounts.full[121], TRANSACTIONS, dateOrigin, dateEnd, 100, 12, 14, "MB-Transfer to account","Withdrawal", 1999, 0, 3, 5566022)


cat(paste('Finished executing at', Sys.time(), '\n'))
end.time <- Sys.time()
print(end.time - start.time)

browser()

sqlServer <- "scorpion.database.windows.net"  #Enter Azure SQL Server
sqlDatabase <- "db"                #Enter Database Name
sqlUser <- "scorpion"             #Enter the SQL User ID
sqlPassword <- "Pa55w.rd1234"        #Enter the User Password
sqlDriver <- "ODBC Driver 17 for SQL Server"        #Leave this Drive Entry

connectionStringSQL <- paste0(
   "Driver=", sqlDriver,
   ";Server=", sqlServer,
   ";Database=", sqlDatabase,
   ";Uid=", sqlUser,
   ";Pwd=", sqlPassword,
   ";Encrypt=yes",
   ";Port=1433")

TRANSACTIONS <- as.data.frame(TRANSACTIONS)


conn <- odbcDriverConnect(connectionStringSQL)
#sqlFetch(conn, "test")
sqlSave(conn, TRANSACTIONS, tablename = 'AccountTransactions', append = FALSE,
         colnames = FALSE, verbose = FALSE)

#dbWriteTable(conn, 'test', TRANSACTIONS)
close(conn)


fwrite(TRANSACTIONS, file = 'transactions1.csv', sep = ";")
