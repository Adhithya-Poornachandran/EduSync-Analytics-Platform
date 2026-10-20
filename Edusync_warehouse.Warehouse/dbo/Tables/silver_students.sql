CREATE TABLE [dbo].[silver_students] (
    [StudentID]          VARCHAR (8000) NULL,
    [FirstName]          VARCHAR (8000) NULL,
    [LastName]           VARCHAR (8000) NULL,
    [Department]         VARCHAR (8000) NULL,
    [JoinYear]           INT            NULL,
    [Email]              VARCHAR (8000) NULL,
    [FullName]           VARCHAR (8000) NULL,
    [EmailDomain]        VARCHAR (8000) NULL,
    [StudentTenureYears] INT            NULL,
    [EmailValid]         BIT            NULL,
    [JoinYearValid]      BIT            NULL,
    [DataQualityStatus]  VARCHAR (8000) NULL
);


GO