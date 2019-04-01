CREATE PROCEDURE [dbo].[GetAllPeopleAndAddresses]
AS
BEGIN
	SELECT PersonId = p.Id,
		p.FirstName,
		p.LastName,
		p.MiddleInitial,
		p.Age,
		p.DateOfBirth,
		p.Gender,
		AddressId = a.Id,
		a.Street,
		a.Apt,
		a.City,
		a.State,
		a.ZipCode
	FROM dbo.Person p
	LEFT JOIN dbo.Address a ON p.Id = a.PersonId
END
