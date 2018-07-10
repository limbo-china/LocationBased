package cn.ac.iie.test.distribution;

import java.util.Scanner;

public class SFTest {

	public static void main(String args[]) {
		Scanner sc = new Scanner(System.in);

		char c = sc.next().charAt(0);

		String s = sc.nextLine();
		// Want to take this string as input after the character

		System.out.println(s);// The compiler bypasses this statement
		System.out.println(c);
		System.out.println("heyyou");
	}

}
