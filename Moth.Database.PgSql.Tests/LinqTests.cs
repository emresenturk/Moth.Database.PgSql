using System;
using System.Linq;
using Moth.Linq;
using Xunit;

namespace Moth.Database.PgSql.Tests
{
    public class Employee : RecordBase<Employee>
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    public class LinqTests : IDisposable
    {
        public LinqTests()
        {
            ContainerTests.Register();
        }


        [Fact]
        public void CanCreateRecord()
        {
            QueryTests.Flush();
            QueryTests.CreateTable();
            var employee = new Employee {FirstName = "Ali", LastName = "Desidero"};
            employee.Create();
            Assert.NotEqual(0, employee.Id);
        }

        [Fact]
        public void CanDoBasicReadOperations()
        {
            QueryTests.Flush();
            QueryTests.CreateTable();
            var employee = new Employee {FirstName = "Ali", LastName = "Handıro"};
            var anotherEmployee = new Employee {FirstName = "Ali", LastName = "Desidero"};
            employee.Create();
            anotherEmployee.Create();
            var employeeRetrieved = Employee.Records.First(e => e.Id == employee.Id);
            var anotherEmployeeRetrieved = Employee.Records.Last(e => e.Id == anotherEmployee.Id);
            Assert.Equal(employee.UId, employeeRetrieved.UId);
            Assert.Equal(anotherEmployee.UId, anotherEmployeeRetrieved.UId);
        }

        [Fact]
        public void UpdateTest()
        {
            QueryTests.Flush();
            QueryTests.CreateTable();
            var employee = new Employee {FirstName = "New", LastName = "Employee"};
            employee.Create();
            employee.FirstName = "Updated New";
            employee.LastName = "Updated Employee";
            employee.Update();
            Assert.NotNull(employee.DateUpdated);
            Assert.Equal(employee.FirstName, "Updated New");
            Assert.Equal(employee.LastName, "Updated Employee");
        }

        [Fact]
        public void DeleteTest()
        {
            QueryTests.Flush();
            QueryTests.CreateTable();
            var employee = new Employee {FirstName = "New", LastName = "Employee"};
            employee.Create();
            employee.Delete();
            var deletedEmployee = Employee.Records.FirstOrDefault(e => e.Id == employee.Id);
            Assert.Null(deletedEmployee);
        }


        public void Dispose()
        {
            QueryTests.Flush();
            DatabaseContainer.Clear();
        }
    }
}