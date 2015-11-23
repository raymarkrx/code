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

		/** * init���������ڹ��캯��������UDAF�ĳ�ʼ�� */

		public void init() {

			state.mSum = new ArrayList<Double>();

			state.mCount = 0;
			state.mindex = 0;

		}

		/** * iterate���մ���Ĳ������������ڲ�����ת���䷵������Ϊboolean * * @param o * @return */

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
		 * * terminatePartial�޲�������Ϊiterate������ת�����󣬷�����ת���ݣ� *
		 * terminatePartial������hadoop��Combiner * * @return
		 */

		public State terminatePartial() {
			// combiner
			return state.mCount == 0 ? null : state;
		}

		/**
		 * * merge����terminatePartial�ķ��ؽ������������merge�������䷵������Ϊboolean * * @param o
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

		/** * terminate�������յľۼ�������� * * @return */

		public Double terminate() {
			Collections.sort(state.mSum);
			return state.mindex > 1 ? null : state.mSum
					.get((int) ((state.mCount - 1) * state.mindex));
		}
	}
}
