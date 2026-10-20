CREATE TABLE [Gold_Schema].[gold_placement_summary] (
    [StudentID]             VARCHAR (8000) NULL,
    [FullName]              VARCHAR (8000) NULL,
    [Department]            VARCHAR (8000) NULL,
    [TotalPlacementRecords] INT            NULL,
    [AppliedRecords]        INT            NULL,
    [EligibleRecords]       INT            NULL,
    [InterviewRecords]      INT            NULL,
    [PlacedRecords]         INT            NULL,
    [IsPlaced]              INT            NULL,
    [AveragePackageLPA]     FLOAT (53)     NULL,
    [HighestPackageLPA]     FLOAT (53)     NULL
);


GO