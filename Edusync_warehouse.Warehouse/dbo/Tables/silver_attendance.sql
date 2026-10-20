CREATE TABLE [dbo].[silver_attendance] (
    [AttendanceID]         VARCHAR (8000) NULL,
    [StudentID]            VARCHAR (8000) NULL,
    [WeekStartDate]        DATE           NULL,
    [ClassesHeld]          INT            NULL,
    [ClassesAttended]      INT            NULL,
    [DataQualityStatus]    VARCHAR (8000) NULL,
    [AttendancePercentage] FLOAT (53)     NULL,
    [ClassesMissed]        INT            NULL,
    [AttendanceStatus]     VARCHAR (8000) NULL
);


GO