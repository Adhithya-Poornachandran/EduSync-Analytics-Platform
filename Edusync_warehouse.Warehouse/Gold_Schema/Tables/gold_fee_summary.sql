CREATE TABLE [Gold_Schema].[gold_fee_summary] (
    [StudentID]         VARCHAR (8000)  NULL,
    [FullName]          VARCHAR (8000)  NULL,
    [Department]        VARCHAR (8000)  NULL,
    [TotalFeeRecords]   INT             NULL,
    [TotalFeesAmount]   DECIMAL (38, 2) NULL,
    [TotalAmountPaid]   DECIMAL (38, 2) NULL,
    [OverdueAmount]     DECIMAL (38, 2) NULL,
    [PaidFeeRecords]    INT             NULL,
    [PendingFeeRecords] INT             NULL,
    [FeePaymentStatus]  VARCHAR (15)    NOT NULL
);


GO