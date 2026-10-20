CREATE TABLE [Gold_Schema].[gold_attendance_summary] (
    [StudentID]                   VARCHAR (8000)   NULL,
    [FullName]                    VARCHAR (8000)   NULL,
    [Department]                  VARCHAR (8000)   NULL,
    [TotalAttendanceRecords]      INT              NULL,
    [TotalClassesHeld]            INT              NULL,
    [TotalClassesAttended]        INT              NULL,
    [TotalClassesMissed]          INT              NULL,
    [OverallAttendancePercentage] NUMERIC (26, 12) NULL,
    [AttendanceRisk]              VARCHAR (13)     NOT NULL
);


GO