CREATE PROCEDURE [dbo].[GetAllPeopleByMinimumAge]
	@MinimumAge TINYINT
AS
BEGIN
	SELECT *
	FROM dbo.Person
	WHERE Age >= @MinimumAge
END
