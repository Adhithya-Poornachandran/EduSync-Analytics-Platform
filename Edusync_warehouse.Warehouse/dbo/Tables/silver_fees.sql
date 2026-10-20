CREATE TABLE [dbo].[silver_fees] (
    [FeeRecordID]        VARCHAR (8000)  NULL,
    [StudentID]          VARCHAR (8000)  NULL,
    [AcademicYear]       VARCHAR (8000)  NULL,
    [TotalFees]          DECIMAL (12, 2) NULL,
    [AmountPaid]         DECIMAL (12, 2) NULL,
    [DueDate]            DATE            NULL,
    [DataQualityStatus]  VARCHAR (8000)  NULL,
    [OutstandingBalance] DECIMAL (13, 2) NULL,
    [PaymentStatus]      VARCHAR (8000)  NULL,
    [DueStatus]          VARCHAR (8000)  NULL
);


GO