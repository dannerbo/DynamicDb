CREATE TABLE [dbo].[Address]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY, 
	[PersonId] INT NOT NULL, 
    [Street] VARCHAR(100) NOT NULL, 
    [Apt] VARCHAR(50) NULL, 
    [City] VARCHAR(50) NOT NULL, 
    [State] CHAR(2) NOT NULL, 
    [ZipCode] CHAR(5) NOT NULL, 
    [CreatedDate] DATETIME NOT NULL DEFAULT GETUTCDATE(), 
    [UpdatedDate] DATETIME NULL, 
    CONSTRAINT [FK_Address_Person] FOREIGN KEY ([PersonId]) REFERENCES [dbo].[Person]([Id])
)

GO

CREATE TRIGGER [dbo].[Trigger_Address_SetUpdatedDate]
ON [dbo].[Address]
FOR UPDATE
AS
BEGIN
    SET NOCOUNT ON

	UPDATE dbo.Address
	SET UpdatedDate = GETUTCDATE()
	FROM inserted
	WHERE dbo.Address.Id = inserted.Id
END

GO
