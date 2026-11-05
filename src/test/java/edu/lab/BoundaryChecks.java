package edu.lab;

import java.math.BigDecimal;
import java.util.Map;

/** Small executable checks for precision, missing values and tier equality. */
public final class BoundaryChecks {
  private static void check(boolean ok, String message) {
    if (!ok) throw new AssertionError(message);
  }

  public static void main(String[] args) {
    check(CreditLab.cents("") == 0, "blank amount");
    check(CreditLab.cents("NaN") == 0, "NaN amount");
    check(CreditLab.cents("null") == 0, "null amount");
    check(CreditLab.cents("12.34") == 1234, "exact cents");
    check(CreditLab.cents("-2.85") == -285, "negative retained");
    boolean rejected = false;
    try { CreditLab.cents("1.001"); } catch (ArithmeticException expected) { rejected = true; }
    check(rejected, "sub-cent precision must not be silently rounded");
    long[] q = {100, 200, 300};
    check(CreditLab.tier(100, q) == 0, "Q25 equality");
    check(CreditLab.tier(101, q) == 1, "above Q25");
    check(CreditLab.tier(200, q) == 1, "Q50 equality");
    check(CreditLab.tier(300, q) == 2, "Q75 equality");
    check(CreditLab.tier(301, q) == 3, "above Q75");
    check(CreditLab.tier(0, new long[] {0,0,0}) == 0, "identical quartiles");
    check(new BigDecimal("0.10").multiply(new BigDecimal("3"))
        .compareTo(new BigDecimal("0.30")) == 0, "mean equality cross product");
    Map<String,Integer> train = CreditLab.schema(true), test = CreditLab.schema(false);
    check(train.size() == 15 && test.size() == 14, "two input schemas");
    check(train.get("CUST_ASSET_AMT") == 10 && test.get("CUST_ASSET_AMT") == 9,
        "test column shift");
    String[] row = "42,1001,,0,0,0,0,0,-2.85,100.00,0,0,Y,N".split(",", -1);
    String[] features = CreditLab.features(row, test, new long[]{1000,2000,3000});
    check(features.length == 14, "feature count");
    check(features[0].equals("ZERO") && features[6].equals("NEG"), "missing/negative bins");
    check(features[10].equals("1001") && features[11].equals("Y"), "categorical positions");
    System.out.println("PASS: missing values, cents precision, negatives, ties, bins and train/test schemas");
  }
}
