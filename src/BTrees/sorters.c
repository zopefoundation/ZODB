/*****************************************************************************

  Copyright (c) 2002 Zope Corporation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

/* Revision information: $Id: sorters.c,v 1.2 2002/05/31 00:21:54 tim_one Exp $ */

/* The only routine here intended to be used outside the file is
   size_t sort_int4_nodups(int *p, size_t n)

   Sort the array of n ints pointed at by p, in place, and also remove
   duplicates.  Return the number of unique elements remaining, which occupy
   a contiguous and monotonically increasing slice of the array starting at p.

   Example:  If the input array is [3, 1, 2, 3, 1, 5, 2], sort_int4_nodups
   returns 4, and the first 4 elements of the array are changed to
   [1, 2, 3, 5].  The content of the remaining array positions is not defined.

   Notes:

   + This is specific to 4-byte signed ints, with endianness natural to the
     platform.

   + 4*n bytes of available heap memory are required for best speed.
*/

#include <stdlib.h>
#include <stddef.h>
#include <malloc.h>
#include <memory.h>
#include <string.h>
#include <assert.h>

/* The type of array elements to be sorted.  Most of the routines don't
   care about the type, and will work fine for any scalar C type (provided
   they're recompiled with element_type appropriately redefined).  However,
   the radix sort has to know everything about the type's internal
   representation.
*/
typedef int element_type;

/* The radixsort is faster than the quicksort for large arrays, but radixsort
   has high fixed overhead, making it a poor choice for small arrays.  The
   crossover point isn't critical, and is sensitive to things like compiler
   and machine cache structure, so don't worry much about this.
*/
#define QUICKSORT_BEATS_RADIXSORT 800U

/* In turn, the quicksort backs off to an insertion sort for very small
   slices.  MAX_INSERTION is the largest slice quicksort leaves entirely to
   insertion.  Because this version of quicksort uses a median-of-3 rule for
   selecting a pivot, MAX_INSERTION must be at least 2 (so that quicksort
   has at least 3 values to look at in a slice).  Again, the exact value here
   isn't critical.
*/
#define MAX_INSERTION 25U

#if MAX_INSERTION < 2U
#   error "MAX_INSERTION must be >= 2"
#endif

/* LSB-first radix sort of the n elements in 'in'.
   'work' is work storage at least as large as 'in'.  Depending on how many
   swaps are done internally, the final result may come back in 'in' or 'work';
   and that pointer is returned.

   radixsort_int4 is specific to signed 4-byte ints, with natural machine
   endianness.
*/
static element_type*
radixsort_int4(element_type *in, element_type *work, size_t n)
{
	/* count[i][j] is the number of input elements that have byte value j
	   in byte position i, where byte position 0 is the LSB.  Note that
	   holding i fixed, the sum of count[i][j] over all j in range(256)
	   is n.
	*/
	size_t count[4][256];
	size_t i;
	int offset, offsetinc;

	/* Which byte position are we working on now?  0=LSB, 1, 2, ... */
	int bytenum;

	assert(sizeof(element_type) == 4);
	assert(in);
	assert(work);

	/* Compute all of count in one pass. */
	memset(count, 0, sizeof(count));
	for (i = 0; i < n; ++i) {
		element_type const x = in[i];
		++count[0][(x      ) & 0xff];
		++count[1][(x >>  8) & 0xff];
		++count[2][(x >> 16) & 0xff];
		++count[3][(x >> 24) & 0xff];
	}

	/* For p an element_type* cast to char*, offset is how much farther we
	   have to go to get to the LSB of the element; this is 0 for little-
	   endian boxes and sizeof(element_type)-1 for big-endian.
	   offsetinc is 1 or -1, respectively, telling us which direction to go
	   from p+offset to get to the element's more-significant bytes.
	*/
	{
		int one = 1;
		if (*(char*)&one) {
			/* Little endian. */
			offset = 0;
			offsetinc = 1;
		}
		else {
			/* Big endian. */
			offset = sizeof(element_type) - 1;
			offsetinc = -1;
		}
	}

	/* The radix sort. */
	for (bytenum = 0;
	     bytenum < sizeof(element_type);
	     ++bytenum, offset += offsetinc) {

		/* Do a stable distribution sort on byte position bytenum,
		   from in to work.  index[i] tells us the work index at which
		   to store the next in element with byte value i.  pinbyte
		   points to the correct byte in the input array.
		*/
	     	size_t index[256];
		unsigned char* pinbyte;
		size_t total = 0;
		size_t *pcount = count[bytenum];

		/* Compute the correct output starting index for each possible
		   byte value.
		*/
		if (bytenum < sizeof(element_type) - 1) {
			for (i = 0; i < 256; ++i) {
				const size_t icount = pcount[i];
				index[i] = total;
				total += icount;
				if (icount == n)
					break;
			}
			if (i < 256) {
				/* All bytes in the current position have value
				   i, so there's nothing to do on this pass.
				*/
				continue;
			}
		}
		else {
			/* The MSB of signed ints needs to be distributed
			   differently than the other bytes, in order
			   0x80, 0x81, ... 0xff, 0x00, 0x01, ... 0x7f
			*/
			for (i = 128; i < 256; ++i) {
				const size_t icount = pcount[i];
				index[i] = total;
				total += icount;
				if (icount == n)
					break;
			}
			if (i < 256)
				continue;
			for (i = 0; i < 128; ++i) {
				const size_t icount = pcount[i];
				index[i] = total;
				total += icount;
				if (icount == n)
					break;
			}
			if (i < 128)
				continue;
		}
		assert(total == n);

		/* Distribute the elements according to byte value.  Note that
		   this is where most of the time is spent.
		   Note:  The loop is unrolled 4x by hand, for speed.  This
		   may be a pessimization someday, but was a significant win
		   on my MSVC 6.0 timing tests.
		*/
		pinbyte = (unsigned char  *)in + offset;
		i = 0;
		/* Reduce number of elements to copy to a multiple of 4. */
		while ((n - i) & 0x3) {
			unsigned char byte = *pinbyte;
			work[index[byte]++] = in[i];
			++i;
			pinbyte += sizeof(element_type);
		}
		for (; i < n; i += 4, pinbyte += 4 * sizeof(element_type)) {
			unsigned char byte1 = *(pinbyte                           );
			unsigned char byte2 = *(pinbyte +     sizeof(element_type));
			unsigned char byte3 = *(pinbyte + 2 * sizeof(element_type));
			unsigned char byte4 = *(pinbyte + 3 * sizeof(element_type));

			element_type in1 = in[i  ];
			element_type in2 = in[i+1];
			element_type in3 = in[i+2];
			element_type in4 = in[i+3];

			work[index[byte1]++] = in1;
			work[index[byte2]++] = in2;
			work[index[byte3]++] = in3;
			work[index[byte4]++] = in4;
		}
		/* Swap in and work (just a pointer swap). */
		{
			element_type *temp = in;
			in = work;
			work = temp;
		}
	}

	return in;
}

/* Remove duplicates from sorted array in, storing exactly one of each distinct
   element value into sorted array out.  It's OK (and expected!) for in == out,
   but otherwise the n elements beginning at in must not overlap with the n
   beginning at out.
   Return the number of elements in out.
*/
static size_t
uniq(element_type *out, element_type *in, size_t n)
{
	size_t i;
	element_type lastelt;
	element_type *pout;

	assert(out);
	assert(in);
	if (n == 0)
		return 0;

	/* i <- first index in 'in' that contains a duplicate.
	   in[0], in[1], ... in[i-1] are unique, but in[i-1] == in[i].
	   Set i to n if everything is unique.
	*/
	for (i = 1; i < n; ++i) {
		if (in[i-1] == in[i])
			break;
	}

	/* in[:i] is unique; copy to out[:i] if needed. */
	assert(i > 0);
	if (in != out)
		memcpy(out, in, i * sizeof(element_type));

	pout = out + i;
	lastelt = in[i-1];  /* safe even when i == n */
	for (++i; i < n; ++i) {
		element_type elt = in[i];
		if (elt != lastelt)
			*pout++ = lastelt = elt;
	}
	return pout - out;
}

/* Straight insertion sort of the n elements starting at 'in'. */
static void
insertionsort(element_type *in, size_t n)
{
	element_type *p, *q;
	element_type minimum;  /* smallest seen so far */
	element_type *plimit = in + n;

	assert(in);
	if (n < 2)
		return;

	minimum = *in;
	for (p = in+1; p < plimit; ++p) {
		/* *in <= *(in+1) <= ... <= *(p-1).  Slide *p into place. */
		element_type thiselt = *p;
		if (thiselt < minimum) {
			/* This is a new minimum.  This saves p-in compares
			   when it happens, but should happen so rarely that
			   it's not worth checking for its own sake:  the
			   point is that the far more popular 'else' branch can
			   exploit that thiselt is *not* the smallest so far.
			*/
			memmove(in+1, in, (p - in) * sizeof(*in));
			*in = minimum = thiselt;
		}
		else {
			/* thiselt >= minimum, so the loop will find a q
			   with *q <= thiselt.  This saves testing q >= in
			   on each trip.  It's such a simple loop that saving
			   a per-trip test is a major speed win.
			*/
			for (q = p-1; *q > thiselt; --q)
				*(q+1) = *q;
			*(q+1) = thiselt;
		}
	}
}

/* The maximum number of elements in the pending-work stack quicksort
   maintains.  The maximum stack depth is approximately log2(n), so
   arrays of size up to approximately MAX_INSERTION * 2**STACKSIZE can be
   sorted.  The memory burden for the stack is small, so better safe than
   sorry.
*/
#define STACKSIZE 60

/* A _stacknode remembers a contiguous slice of an array that needs to sorted.
   lo must be <= hi, and, unlike Python array slices, this includes both ends.
*/
struct _stacknode {
	element_type *lo;
	element_type *hi;
};

static void
quicksort(element_type *plo, size_t n)
{
	element_type *phi;

	/* Swap two array elements. */
	element_type _temp;
#define SWAP(P, Q) (_temp = *(P), *(P) = *(Q), *(Q) = _temp)

	/* Stack of pending array slices to be sorted. */
	struct _stacknode stack[STACKSIZE];
	struct _stacknode *stackfree = stack;	/* available stack slot */

	/* Push an array slice on the pending-work stack. */
#define PUSH(PLO, PHI)					\
	do {						\
		assert(stackfree - stack < STACKSIZE);	\
		assert((PLO) <= (PHI));			\
		stackfree->lo = (PLO);			\
		stackfree->hi = (PHI);			\
		++stackfree;				\
	} while(0)

	assert(plo);
	phi = plo + n - 1;

	for (;;) {
		element_type pivot;
		element_type *pi, *pj;

		assert(plo <= phi);
		n = phi - plo + 1;
		if (n <= MAX_INSERTION) {
			/* Do a small insertion sort.  Contra Knuth, we do
			   this now instead of waiting until the end, because
			   this little slice is likely still in cache now.
			*/
			element_type *p, *q;
			element_type minimum = *plo;

			for (p = plo+1; p <= phi; ++p) {
				/* *plo <= *(plo+1) <= ... <= *(p-1).
				   Slide *p into place. */
				element_type thiselt = *p;
				if (thiselt < minimum) {
					/* New minimum. */
					memmove(plo+1,
						plo,
						(p - plo) * sizeof(*p));
					*plo = minimum = thiselt;
				}
				else {
					/* thiselt >= minimum, so the loop will
					   find a q with *q <= thiselt.
					*/
					for (q = p-1; *q > thiselt; --q)
						*(q+1) = *q;
					*(q+1) = thiselt;
				}
			}

			/* Pop another slice off the stack. */
			if (stack == stackfree)
				break;	/* no more slices -- we're done */
			--stackfree;
			plo = stackfree->lo;
			phi = stackfree->hi;
			continue;
		}

		/* Parition the slice.
		   For pivot, take the median of the leftmost, rightmost, and
		   middle elements.  First sort those three; then the median
		   is the middle one.  For technical reasons, the middle
		   element is swapped to plo+1 first (see Knuth Vol 3 Ed 2
		   section 5.2.2 exercise 55 -- reverse-sorted arrays can
		   take quadratic time otherwise!).
		*/
		{
			element_type *plop1 = plo + 1;
			element_type *pmid = plo + (n >> 1);

			assert(plo < pmid && pmid < phi);
			SWAP(plop1, pmid);

			/* Sort plo, plop1, phi. */
			/* Smaller of rightmost two -> middle. */
			if (*plop1 > *phi)
				SWAP(plop1, phi);
			/* Smallest of all -> left; if plo is already the
			   smallest, the sort is complete.
			*/
			if (*plo > *plop1) {
				SWAP(plo, plop1);
				/* Largest of all -> right. */
				if (*plop1 > *phi)
					SWAP(plop1, phi);
			}
			pivot = *plop1;
			pi = plop1;
		}
		assert(*plo <= pivot);
		assert(*pi == pivot);
		assert(*phi >= pivot);
		pj = phi;

		/* Partition wrt pivot.  This is the time-critical part, and
		   nearly every decision in the routine aims at making this
		   loop as fast as possible -- even small points like
		   arranging that all loop tests can be done correctly at the
		   bottoms of loops instead of the tops, and that pointers can
		   be derefenced directly as-is (without fiddly +1 or -1).
		   The aim is to make the C here so simple that a compiler
		   has a good shot at doing as well as hand-crafted assembler.
		*/
		for (;;) {
			/* Invariants:
			   1. pi < pj.
			   2. All elements at plo, plo+1 .. pi are <= pivot.
			   3. All elements at pj, pj+1 .. phi are >= pivot.
			   4. There is an element >= pivot to the right of pi.
			   5. There is an element <= pivot to the left of pj.

			   Note that #4 and #5 save us from needing to check
			   that the pointers stay in bounds.
			*/
			assert(pi < pj);

			do { ++pi; } while (*pi < pivot);
			assert(pi <= pj);

			do { --pj; } while (*pj > pivot);
			assert(pj >= pi - 1);

			if (pi < pj)
				SWAP(pi, pj);
			else
				break;
		}
		assert(plo+1 < pi && pi <= phi);
		assert(plo < pj && pj < phi);
		assert(*pi >= pivot);
		assert( (pi == pj && *pj == pivot) ||
			(pj + 1 == pi && *pj <= pivot) );

		/* Swap pivot into its final position, pj. */
		assert(plo[1] == pivot);
		plo[1] = *pj;
		*pj = pivot;

		/* Subfiles are from plo to pj-1 inclusive, and pj+1 to phi
		   inclusive.  Push the larger one, and loop back to do the
		   smaller one directly.
		*/
		if (pj - plo >= phi - pj) {
			PUSH(plo, pj-1);
			plo = pj+1;
		}
		else {
			PUSH(pj+1, phi);
			phi = pj-1;
		}
	}

#undef PUSH
#undef SWAP
}

/* Sort p and remove duplicates, as fast as we can. */
static size_t
sort_int4_nodups(int *p, size_t n)
{
	size_t nunique;
	element_type *work;

	assert(sizeof(int) == sizeof(element_type));
	assert(p);

	/* Use quicksort if the array is small, OR if malloc can't find
	   enough temp memory for radixsort.
	*/
	work = NULL;
	if (n > QUICKSORT_BEATS_RADIXSORT)
		work = (element_type *)malloc(n * sizeof(element_type));

	if (work) {
		element_type *out = radixsort_int4(p, work, n);
		nunique = uniq(p, out, n);
		free(work);
	}
	else {
		quicksort(p, n);
		nunique = uniq(p, p, n);
	}

	return nunique;
}
