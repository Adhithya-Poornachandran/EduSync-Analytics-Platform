CREATE TABLE [dbo].[silver_placements] (
    [PlacementRecordID] VARCHAR (8000) NULL,
    [StudentID]         VARCHAR (8000) NULL,
    [Company]           VARCHAR (8000) NULL,
    [PackageLPA]        FLOAT (53)     NULL,
    [Status]            VARCHAR (8000) NULL,
    [UpdatedDate]       DATE           NULL,
    [DataQualityStatus] VARCHAR (8000) NULL,
    [PlacementFlag]     INT            NULL,
    [PlacementStage]    VARCHAR (8000) NULL,
    [PackageCategory]   VARCHAR (8000) NULL,
    [UpdateYear]        INT            NULL
);


GO