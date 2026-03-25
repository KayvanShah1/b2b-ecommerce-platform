# SQL ETL Project

**Est. time/effort: 10 hours**

## Task Description
**Expected delivery time: 7 days**

## Task scope and expectations
The purpose of this task is to build a functional, real-life application that meets all of the specified requirements.
Our aim is to evaluate your ability to implement some real-life functionalities by paying attention to details and following good development practices.
We seek to assess your proficiency in these areas:
- Establishing a proper project infrastructure;
- Demonstrating knowledge of basic problem-solving;
- Effectively utilizing your preferred frameworks and libraries.

## Task Details
You need to design a data warehouse/data store ETL/ELT system to manage data for a B2B e-commerce site. The data storage system has three defined data sources that feed the data to the system:
- B2B platform database  
- Log data from the web server  
- Marketing lead spreadsheet file  

### B2B platform
Companies may have many end Customers. The B2B platform allows them to buy Products from qualified Suppliers and sell them to end Customers.
- End Customers are people who are identified by their document number, full name, and date of birth.  
- Companies are identified by CUIT number (a unique identifier), name.  
- Suppliers are just a different type of company, and they expose a list of Products and default prices.  
- Each Company can define its own price list (catalog) using Products from many Suppliers.  
- They can also place Orders to the platform indicating which end customer has to receive the goods.

### Weblog data
The clients are accessing a B2B website from various devices and geo-locations. This data is represented in the combined log format.
**LogFormat** `"%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"" combined`

- `%h` is the remote host (ie the client IP)  
- `%l` is the identity of the user determined by ident (not usually used since not reliable).  
- `%u` is the username determined by HTTP authentication  
- `%t` is the time the request was received.  
- `%r` is the request line from the client. ("GET / HTTP/1.0")  
- `%>s` is the status code sent from the server to the client (200, 404, etc.)  
- `%b` is the size of the response to the client (in bytes)  
- Referer is the Referer header of the HTTP request (containing the URL of the page from which this request was initiated) if any is present, and "-" otherwise.  
- User-agent is the browser identification string.  

**For our project, the relevant data of this data source is the client IP, username, time, and user-agent. You can omit other data as non-relevant data.**

**The target datastore is the foundation for these reports:**
- What are the most popular used devices for B2B clients (top 5)  
- What are the most popular products in the country from which most users log into  
- All sales of B2B platform displayed monthly for the last year

## Requirements
For the project solution, please prepare the following:
- Database implementation with generated data for the B2B database source  
- Weblog generated via script in a language of your choosing  
- A target database that represents the data warehouse or data mart, you can choose relation or NoSQL solution  
- ETL/ELT* process with transformations that will:  
- Demonstrate the ability to transform large data size  
- Be restartable if the jobs or sub-job fails  
- Fill the initial load of the target datastore  
- Handle erroneous data  
- Track ETL/ELT metadata (when did the load start, break, and finish)  
- Transform the data into a readable data format for reporting.  

\* Use any ETL/ELT tool or hand code the ETL/ELT process

## Milestones and task delivery
- The deadline to submit your completed project is 7 days from the moment you receive the project requirements.  
- It means the project code must be submitted within 7 days from the moment that it was delivered to you by email.  
- If you schedule your final interview after the 7-day deadline, make sure to submit your completed project and all code to the private repository before the deadline  
- Everything that is submitted after the deadline will not be taken into consideration.  
- To ensure sufficient review time, please commit all code or documents at least 6 hours before the meeting. Submissions after this time will not be considered.  
- **Short project video** - after you finish the project, please provide us with a simple and short video recording of the functionality and submit it to the GIT repository. Please do not spend too much time on this step. Our goal is to see your project presentation as soon as possible and how you would showcase a quick intro for the client. So, please choose any free or preinstalled software that can help you do this.  
- Please join the meeting room for this final interview on time. If you miss your interview without prior notice, your application may be closed.
