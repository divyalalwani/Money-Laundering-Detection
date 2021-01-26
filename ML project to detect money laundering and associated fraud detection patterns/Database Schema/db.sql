SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[accountflag](
	[accountNumber] [varchar](50) NULL,
	[institution] [varchar](50) NULL,
	[transit] [varchar](50) NULL,
	[flag] [decimal](20, 0) NULL
) ON [PRIMARY]
GO




SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[newfilteredrow](
	[rank] [int] NULL,
	[accountNumber] [varchar](50) NULL,
	[institution] [varchar](50) NULL,
	[transit] [varchar](50) NULL,
	[date] [varchar](50) NULL,
	[transaction] [varchar](50) NULL,
	[transactionType] [varchar](50) NULL,
	[openingBalance] [varchar](50) NULL,
	[amountsWithdrawn] [varchar](50) NULL,
	[amountsDeposited] [varchar](50) NULL,
	[balance] [varchar](50) NULL,
	[mlIdentifier] [varchar](50) NULL,
	[meanTranscPerDisctAccntPerMonth] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_OA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_OA] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_OA] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_OA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_MA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_MA] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_MA] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_MA] [decimal](20, 0) NULL,
	[frequentDepositSum] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupDepositeddata] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupWithdrawndata] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupDepositeddata] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupWithdrawndata] [decimal](20, 0) NULL,
	[flaggedAccntVal] [decimal](20, 0) NULL,
	[meanTranscPerDisctAccntPerMonth_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_OA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_OA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_OA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_OA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_MA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_MA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_MA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_MA_Rank] [decimal](20, 0) NULL,
	[frequentDepositSum_Rank] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupDepositeddata_Rank] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupWithdrawndata_Rank] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupDepositeddata_Rank] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupWithdrawndata_Rank] [decimal](20, 0) NULL,
	[flaggedAccntVal_Rank] [decimal](20, 0) NULL,
	[accountDetails] [varchar](50) NULL
) ON [PRIMARY]
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[newmlData](
	[uniqueID] [int] IDENTITY(1,1) NOT NULL,
	[rank] [int] NULL,
	[accountNumber] [varchar](50) NULL,
	[institution] [varchar](50) NULL,
	[transit] [varchar](50) NULL,
	[date] [varchar](50) NULL,
	[transaction] [varchar](50) NULL,
	[transactionType] [varchar](50) NULL,
	[openingBalance] [varchar](50) NULL,
	[amountsWithdrawn] [varchar](50) NULL,
	[amountsDeposited] [varchar](50) NULL,
	[balance] [varchar](50) NULL,
	[mlIdentifier] [varchar](50) NULL,
	[meanTranscPerDisctAccntPerMonth] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_OA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_OA] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_OA] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_OA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_MA] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_MA] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_MA] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_MA] [decimal](20, 0) NULL,
	[frequentDepositSum] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupDepositeddata] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupWithdrawndata] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupDepositeddata] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupWithdrawndata] [decimal](20, 0) NULL,
	[flaggedAccntVal] [decimal](20, 0) NULL,
	[meanTranscPerDisctAccntPerMonth_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_OA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_OA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_OA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_OA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsDeposited_MA_Rank] [decimal](20, 0) NULL,
	[sixmonthTotalamountsWithdrawn_MA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsDeposited_MA_Rank] [decimal](20, 0) NULL,
	[threemonthTotalamountsWithdrawn_MA_Rank] [decimal](20, 0) NULL,
	[frequentDepositSum_Rank] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupDepositeddata_Rank] [decimal](20, 0) NULL,
	[sixmonthsagoaccountDetailsgroupWithdrawndata_Rank] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupDepositeddata_Rank] [decimal](20, 0) NULL,
	[threemonthsagoaccountDetailsgroupWithdrawndata_Rank] [decimal](20, 0) NULL,
	[flaggedAccntVal_Rank] [decimal](20, 0) NULL,
	[Action] [varchar](50) NULL,
	[accountDetails] [varchar](50) NULL
) ON [PRIMARY]
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[transactions](
	[accountNumber] [varchar](50) NULL,
	[institution] [varchar](50) NULL,
	[transit] [varchar](50) NULL,
	[date] [varchar](50) NULL,
	[transaction] [varchar](50) NULL,
	[accountDetails] [varchar](50) NULL,
	[transactionType] [varchar](50) NULL,
	[openingBalance] [varchar](50) NULL,
	[amountsWithdrawn] [varchar](50) NULL,
	[amountsDeposited] [varchar](50) NULL,
	[balance] [varchar](50) NULL,
	[mlIdentifier] [varchar](50) NULL
) ON [PRIMARY]
GO
