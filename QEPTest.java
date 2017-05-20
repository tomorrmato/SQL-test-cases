package tests;
import java.io.*;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;


public class QEPTest extends TestDriver {
 
 	private static final String TEST_NAME = "Query Evaluation Pipelines tests";
	private static final int SUPER_SIZE = 2000;

	/** employee table schema. */
	private static Schema s_employee;
    private static HeapFile employee;
    
	/** department table schema. */
	private static Schema s_department;
    private static HeapFile department;
    

	public static void main(String argv[]) {

		QEPTest qept = new QEPTest();
		qept.create_minibase();

		String data_path = argv[0]; 

		// initialize schema for the "Employee" table
		s_employee = new Schema(5);
		s_employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_employee.initField(1, AttrType.STRING, 20, "Name");
		s_employee.initField(2, AttrType.FLOAT, 4, "Age");
		s_employee.initField(3, AttrType.FLOAT, 4, "Salary");
		s_employee.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "department" table
		s_department = new Schema(4);
		s_department.initField(0, AttrType.INTEGER, 4, "DeptId");
		s_department.initField(1, AttrType.STRING, 20, "Name");
		s_department.initField(2, AttrType.FLOAT, 4, "MinSalary");
		s_department.initField(3, AttrType.FLOAT, 4, "MaxSalary");

		// load data into "Employee" table
		String thisFilePath = data_path + "/Employee.txt";
        BufferedReader br = null;
        String line = "";
        Tuple tuple = new Tuple(s_employee);
        employee = new HeapFile(null);
		try {
			br = new BufferedReader(new FileReader(thisFilePath));
            line = br.readLine(); // skip header to read data
			while ((line = br.readLine()) != null) {
				String[] thisLine = line.split(",");
				tuple.setAllFields(Integer.parseInt(thisLine[0].trim()), thisLine[1].trim(), Float.parseFloat(thisLine[2].trim()), Float.parseFloat(thisLine[3].trim()), Integer.parseInt(thisLine[4].trim()));
				tuple.insertIntoFile(employee);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// load data into "department" table
		thisFilePath = data_path + "/Department.txt";
        br = null;
        line = "";
        tuple = new Tuple(s_department);
        department = new HeapFile(null);
		try {
			br = new BufferedReader(new FileReader(thisFilePath));
            line = br.readLine(); // skip header to read data too
			while ((line = br.readLine()) != null) {
				String[] thisLine = line.split(",");
				tuple.setAllFields(Integer.parseInt(thisLine[0].trim()), thisLine[1].trim(), Float.parseFloat(thisLine[2].trim()), Float.parseFloat(thisLine[3].trim()));
				tuple.insertIntoFile(department);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		status &= qept.test1();
		status &= qept.test2();
		status &= qept.test3();
		status &= qept.test4();

		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}
	} 

	
	protected boolean test1() {
        try {
        	System.out.println("******************************************************************\n");
			System.out.println("\nTest 1: Display employee ID, names and age");
			System.out.println("");
			System.out.println("select E.empid, E.name, E.age");
			System.out.println("from Employee E;");
			System.out.println("");
			initCounts();
			saveCounts(null);
		
			FileScan scan = new FileScan(s_employee, employee);
			Projection pro = new Projection(scan, 0, 1, 2);
			pro.execute();
			saveCounts("test1");
			
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;
			
		} catch (Exception exc){
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	} 

 
	protected boolean test2() {
		try {
			System.out.println("******************************************************************\n");
			System.out.println("\nTest 2: Display departments names, minsalary and maxsalary where minsalary = maxsalary");
			System.out.println("");
			System.out.println("select D.name, D.minsalary, D.maxsalary");
			System.out.println("from Department D");
			System.out.println("where D.minsalary = D.maxsalary");
			System.out.println("");

			initCounts();
			saveCounts(null);
			
			FileScan scan = new FileScan(s_department, department);
			Predicate[] preds = new Predicate[] { new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3) };
			Selection sel = new Selection(scan, preds);
			Projection pro = new Projection(sel, 1, 2, 3);
			pro.execute();
			saveCounts("test2");
			
			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;
			
		} catch (Exception exc){
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}


	protected boolean test3() {
		try {
			System.out.println("******************************************************************\n");
			System.out.println("\nTest 3: Display employee name, his department and maxsalary of the department");
			System.out.println("");
			System.out.println("select E.name, D.name, D.maxsalary");
			System.out.println("from Employee E, Department D");
			System.out.println("where E.deptid = D.deptid");
			System.out.println("");

			initCounts();
			saveCounts(null);
			
			HashJoin join = new HashJoin(new FileScan(s_employee, employee), new FileScan(s_department, department), 4, 0);
			Projection pro = new Projection(join, 1, 6, 8);
			pro.execute();
			saveCounts("test3");
			
			System.out.print("\n\nTest 3 completed without exception.");
			return PASS;
			
		} catch (Exception exc){
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}


	protected boolean test4() {
		try {
			System.out.println("******************************************************************\n");
			System.out.println("\nTest 4: Display employee name whose Salary is greater than department maxsalary");
		    System.out.println("");
			System.out.println("select E.name, E.salary, D.maxsalary");
			System.out.println("from Employee E, Department D");
			System.out.println("where E.salary > D.maxsalary");
			System.out.println("");

		    HashJoin join = new HashJoin(new FileScan(s_employee, employee),
		        new FileScan(s_department, department), 4, 0);
		    Selection sel = new Selection(join, new Predicate(AttrOperator.GT,
			    AttrType.FIELDNO, 3, AttrType.FIELDNO, 8));
		    Projection pro = new Projection(sel, 1, 3, 8);	
			pro.execute();
			
			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;

		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	} 

}
