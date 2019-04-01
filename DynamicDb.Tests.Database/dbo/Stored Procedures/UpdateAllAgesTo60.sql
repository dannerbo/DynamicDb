CREATE PROCEDURE [dbo].[UpdateAllAgesTo60]
AS
BEGIN
	UPDATE dbo.Person SET Age = 60
END
