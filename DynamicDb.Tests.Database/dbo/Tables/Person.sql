CREATE TABLE [dbo].[Person]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY, 
    [FirstName] VARCHAR(50) NOT NULL, 
    [LastName] VARCHAR(50) NOT NULL, 
    [MiddleInitial] CHAR(1) NULL, 
    [Age] TINYINT NOT NULL, 
    [DateOfBirth] DATE NOT NULL, 
	[Gender] TINYINT NOT NULL, 
    [CreatedDate] DATETIME NOT NULL DEFAULT GETUTCDATE(), 
    [UpdatedDate] DATETIME NULL
)

GO

CREATE TRIGGER [dbo].[Trigger_Person_SetUpdatedDate]
ON [dbo].[Person]
FOR UPDATE
AS
BEGIN
    SET NOCOUNT ON

	UPDATE dbo.Person
	SET UpdatedDate = GETUTCDATE()
	FROM inserted
	WHERE dbo.Person.Id = inserted.Id
END

GO
