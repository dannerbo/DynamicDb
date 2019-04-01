CREATE PROCEDURE [dbo].[UpdateAllAges]
	@Age TINYINT
AS
BEGIN
	UPDATE dbo.Person SET Age = @Age
END
