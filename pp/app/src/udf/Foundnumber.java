package udf;

import java.util.ArrayList;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class Foundnumber extends UDAF {

	public static class State {
		private long mCount;
		private List<Double> mSum;
		private double mindex;

	}

	public static class FoundnumberEvaluator implements UDAFEvaluator {

		State state;

		public FoundnumberEvaluator() {

			super();

			state = new State();

			init();

		}

		/** * init函数类似于构造函数，用于UDAF的初始化 */

		public void init() {

			state.mSum = new ArrayList<Double>();

			state.mCount = 0;
			state.mindex = 0;

		}

		/** * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return */

		public boolean iterate(Double o, Double i) {

			if (o != null) {

				state.mSum.add(o);

				state.mCount++;

			}
			if (state.mindex == 0) {
				state.mindex = i;
			}
			return true;

		}

		/**
		 * * terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据， *
		 * terminatePartial类似于hadoop的Combiner * * @return
		 */

		public State terminatePartial() {
			// combiner
			return state.mCount == 0 ? null : state;
		}

		/**
		 * * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o
		 * * @return
		 */

		public boolean merge(State o) {
			if (o != null) {
				state.mCount += o.mCount;
				for (int i = 0; i < o.mSum.size(); i++) {
					state.mSum.add(o.mSum.get(i));
				}
			}
			return true;
		}

		/** * terminate返回最终的聚集函数结果 * * @return */

		public Double terminate() {
			Collections.sort(state.mSum);
			return state.mindex > 1 ? null : state.mSum
					.get((int) ((state.mCount - 1) * state.mindex));
		}
	}
}
