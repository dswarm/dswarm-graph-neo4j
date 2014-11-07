/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.versioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Range implements Comparable<Range> {

	public static final Range	NIL	= new Range(-1, -1);

	private final int			from;
	private final int			to;

	public Range(final int from, final int to) {
		if (from > to) {
			throw new IllegalArgumentException(String.format("From [%d] was after To [%d].", from, to));
		}
		this.from = from;
		this.to = to;
	}

	public int from() {
		return from;
	}

	public int to() {
		return to;
	}

	public Range intersect(final Range other) {
		if (to < other.from || from > other.to) {
			return Range.NIL;
		}
		if (to == other.from) {
			return new Range(to, to);
		}
		if (from == other.to) {
			return new Range(from, from);
		}
		final int newFrom = Math.max(from, other.from);
		final int newTo = Math.min(to, other.to);
		return new Range(newFrom, newTo);
	}

	public Set<Range> union(final Range other) {
		if (intersect(other) == Range.NIL) {
			return Range.asSet(this, other);
		}
		if (equals(other)) {
			return Range.asSet(this);
		}
		return Range.asSet(realUnion(other));
	}

	public static Set<Range> asSet(final Range... items) {
		return new HashSet<>(Arrays.asList(items));
	}

	private Range realUnion(final Range other) {
		final int newFrom = Math.min(from, other.from);
		final int newTo = Math.max(to, other.to);
		return new Range(newFrom, newTo);
	}

	public boolean overlaps(final Range other) {
		return !intersect(other).equals(Range.NIL);
	}

	public boolean contains(final int point) {
		return point >= from && point < to;
	}

	@Override
	public String toString() {
		return String.format("Range[%d,%d]", from, to);
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final Range range = (Range) o;

		return from == range.from && to == range.to;

	}

	@Override
	public int hashCode() {
		int result = (int) (from ^ (from >>> 32));
		result = 31 * result + (int) (to ^ (to >>> 32));
		return result;
	}

	@Override
	public int compareTo(final Range o) {
		if (o == null) {
			return -1;
		}
		return Integer.valueOf(from).compareTo(o.from);
	}

	public static List<Range> compactRanges(final Collection<Range> ranges) {
		final List<Range> result = new ArrayList<>();
		Range.takeNextRangeAndMergeOverlappingRanges(new ArrayList<>(ranges), result);
		Collections.sort(result);
		return result;
	}

	private static void takeNextRangeAndMergeOverlappingRanges(final List<Range> rangeList, final List<Range> result) {
		if (rangeList.isEmpty()) {
			return;
		}
		result.add(Range.consumeAndMergeOverlappingRanges(rangeList.remove(0), rangeList));
		Range.takeNextRangeAndMergeOverlappingRanges(rangeList, result);
	}

	private static Range consumeAndMergeOverlappingRanges(Range first, final List<Range> rangeList) {
		for (int i = 0; i < rangeList.size(); i++) {
			final Range rangeInList = rangeList.get(i);
			if (first.overlaps(rangeInList)) {
				first = first.union(rangeInList).iterator().next();
				rangeList.remove(i);
				return Range.consumeAndMergeOverlappingRanges(first, rangeList);
			}
		}
		return first;
	}

	public static Range range(final int from, final int to) {
		return new Range(from, to);
	}

	public static Range range(final int from) {
		return new Range(from, Integer.MAX_VALUE);
	}
}
