CREATE TABLE [Gold_Schema].[gold_student_performance] (
    [StudentID]          VARCHAR (8000)  NULL,
    [FullName]           VARCHAR (8000)  NULL,
    [Department]         VARCHAR (8000)  NULL,
    [JoinYear]           INT             NULL,
    [TotalSubjects]      INT             NULL,
    [SubjectsPassed]     INT             NULL,
    [SubjectsFailed]     INT             NULL,
    [AveragePercentage]  FLOAT (53)      NULL,
    [PassRatePercentage] NUMERIC (38, 6) NULL,
    [PerformanceStatus]  VARCHAR (8)     NOT NULL
);


GO