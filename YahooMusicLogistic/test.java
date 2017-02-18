  public int diff_commonPrefix(String str1, String str2) {
    int n = Math.min(str1.length(), str2.length());
    for (int i = 0; i < n; i++) {
      if (str1.charAt(i) != str2.charAt(i)) {
        return i;
      }
    }
    return n;
  }

  public int diff_commonSuffix(String str1, String str2) {
    int str1_length = str1.length();
    int str2_length = str2.length();
    int n = Math.min(str1_length, str2_length);
    for (int i = 1; i <= n; i++) {
      if (str1.charAt(str1_length - i) != str2.charAt(str2_length - i)) {
        return i - 1;
      }
    }
    return n;
  }

  protected int diff_commonOverlap(String str1, String str2) {
    int str1_length = str1.length();
    int str2_length = str2.length();
    if (str1_length == 0 || str2_length == 0) {
      return 0;
    }
    if (str1_length > str2_length) {
      str1 = str1.substring(str1_length - str2_length);
    } else if (str1_length < str2_length) {
      str2 = str2.substring(0, str1_length);
    }
    int text_length = Math.min(str1_length, str2_length);
    if (str1.equals(str2)) {
      return text_length;
    }

    int best = 0;
    int length = 1;
    while (true) {
      String pattern = str1.substring(text_length - length);
      int found = str2.indexOf(pattern);
      if (found == -1) {
        return best;
      }
      length += found;
      if (found == 0 || str1.substring(text_length - length).equals(
          str2.substring(0, length))) {
        best = length;
        length++;
      }
    }
  }

  protected String[] diff_halfMatch(String str1, String str2) {
    if (Diff_Timeout <= 0) {
      return null;
    }
    String longtext = str1.length() > str2.length() ? str1 : str2;
    String shorttext = str1.length() > str2.length() ? str2 : str1;
    if (longtext.length() < 4 || shorttext.length() * 2 < longtext.length()) {
      return null;
    }

    String[] hm1 = diff_halfMatchI(longtext, shorttext,
                                   (longtext.length() + 3) / 4);

    String[] hm2 = diff_halfMatchI(longtext, shorttext,
                                   (longtext.length() + 1) / 2);
    String[] hm;
    if (hm1 == null && hm2 == null) {
      return null;
    } else if (hm2 == null) {
      hm = hm1;
    } else if (hm1 == null) {
      hm = hm2;
    } else {
      hm = hm1[4].length() > hm2[4].length() ? hm1 : hm2;
    }

    if (str1.length() > str2.length()) {
      return hm;
    } else {
      return new String[]{hm[2], hm[3], hm[0], hm[1], hm[4]};
    }
  }

  private String[] diff_halfMatchI(String longtext, String shorttext, int i) {
    String seed = longtext.substring(i, i + longtext.length() / 4);
    int j = -1;
    String best_common = "";
    String best_longtext_a = "", best_longtext_b = "";
    String best_shorttext_a = "", best_shorttext_b = "";
    while ((j = shorttext.indexOf(seed, j + 1)) != -1) {
      int prefixLength = diff_commonPrefix(longtext.substring(i),
                                           shorttext.substring(j));
      int suffixLength = diff_commonSuffix(longtext.substring(0, i),
                                           shorttext.substring(0, j));
      if (best_common.length() < suffixLength + prefixLength) {
        best_common = shorttext.substring(j - suffixLength, j)
            + shorttext.substring(j, j + prefixLength);
        best_longtext_a = longtext.substring(0, i - suffixLength);
        best_longtext_b = longtext.substring(i + prefixLength);
        best_shorttext_a = shorttext.substring(0, j - suffixLength);
        best_shorttext_b = shorttext.substring(j + prefixLength);
      }
    }
    if (best_common.length() * 2 >= longtext.length()) {
      return new String[]{best_longtext_a, best_longtext_b,
                          best_shorttext_a, best_shorttext_b, best_common};
    } else {
      return null;
    }
  }

  public void diff_cleanupSemantic(LinkedList<Diff> linkedListDiffs) {
    if (linkedListDiffs.isEmpty()) {
      return;
    }
    boolean change_or_not = false;
    Stack<Diff> Diff_stack_equalities = new Stack<Diff>();  // Stack of qualities.
    String lastequality = null; // Always equal to Diff_stack_equalities.lastElement().text
    ListIterator<Diff> pointer = linkedListDiffs.listIterator();
    int length_insertions1 = 0;
    int length_deletions1 = 0;
    int length_insertions2 = 0;
    int length_deletions2 = 0;
    Diff currentDIff = pointer.next();
    while (currentDIff != null) {
      if (currentDIff.operation == Operation.EQUAL) {
        Diff_stack_equalities.push(currentDIff);
        length_insertions1 = length_insertions2;
        length_deletions1 = length_deletions2;
        length_insertions2 = 0;
        length_deletions2 = 0;
        lastequality = currentDIff.text;
      } else {
        if (currentDIff.operation == Operation.INSERT) {
          length_insertions2 += currentDIff.text.length();
        } else {
          length_deletions2 += currentDIff.text.length();
        }

        if (lastequality != null && (lastequality.length()
            <= Math.max(length_insertions1, length_deletions1))
            && (lastequality.length()
                <= Math.max(length_insertions2, length_deletions2))) {
          while (currentDIff != Diff_stack_equalities.lastElement()) {
            currentDIff = pointer.previous();
          }
          pointer.next();

          pointer.set(new Diff(Operation.DELETE, lastequality));
          pointer.add(new Diff(Operation.INSERT, lastequality));

          Diff_stack_equalities.pop();
          if (!Diff_stack_equalities.empty()) {
            Diff_stack_equalities.pop();
          }
          if (Diff_stack_equalities.empty()) {
            while (pointer.hasPrevious()) {
              pointer.previous();
            }
          } else {
            currentDIff = Diff_stack_equalities.lastElement();
            while (currentDIff != pointer.previous()) {
            }
          }

          length_insertions1 = 0;
          length_insertions2 = 0;
          length_deletions1 = 0;
          length_deletions2 = 0;
          lastequality = null;
          change_or_not = true;
        }
      }
      currentDIff = pointer.hasNext() ? pointer.next() : null;
    }

    if (change_or_not) {
      diff_cleanupMerge(linkedListDiffs);
    }
    diff_cleanupSemanticLossless(linkedListDiffs);

    Diff prevDiff = null;
    currentDIff = null;
    if (pointer.hasNext()) {
      prevDiff = pointer.next();
      if (pointer.hasNext()) {
        currentDIff = pointer.next();
      }
    }
    while (currentDIff != null) {
      if (prevDiff.operation == Operation.DELETE &&
          currentDIff.operation == Operation.INSERT) {
        String deletion = prevDiff.text;
        String insertion = currentDIff.text;
        int overlap_length1 = this.diff_commonOverlap(deletion, insertion);
        int overlap_length2 = this.diff_commonOverlap(insertion, deletion);
        if (overlap_length1 >= overlap_length2) {
          if (overlap_length1 >= deletion.length() / 2.0 ||
              overlap_length1 >= insertion.length() / 2.0) {
            pointer.previous();
            pointer.add(new Diff(Operation.EQUAL,
                                 insertion.substring(0, overlap_length1)));
            prevDiff.text =
                deletion.substring(0, deletion.length() - overlap_length1);
            currentDIff.text = insertion.substring(overlap_length1);
          }
        } else {
          if (overlap_length2 >= deletion.length() / 2.0 ||
              overlap_length2 >= insertion.length() / 2.0) {
            pointer.previous();
            pointer.add(new Diff(Operation.EQUAL,
                                 deletion.substring(0, overlap_length2)));
            prevDiff.operation = Operation.INSERT;
            prevDiff.text =
              insertion.substring(0, insertion.length() - overlap_length2);
            currentDIff.operation = Operation.DELETE;
            currentDIff.text = deletion.substring(overlap_length2);
          }
        }
        currentDIff = pointer.hasNext() ? pointer.next() : null;
      }
      prevDiff = currentDIff;
      currentDIff = pointer.hasNext() ? pointer.next() : null;
    }
  }

  public void diff_cleanupSemanticLossless(LinkedList<Diff> linkedListDiffs) {
    String equality1, edit, equality2;
    String commonString;
    int commonOffset;
    int score, bestScore;
    String bestEquality1, bestEdit, bestEquality2;
    ListIterator<Diff> pointer = linkedListDiffs.listIterator();
    Diff prevDiff = pointer.hasNext() ? pointer.next() : null;
    Diff currentDIff = pointer.hasNext() ? pointer.next() : null;
    Diff nextDiff = pointer.hasNext() ? pointer.next() : null;
    while (nextDiff != null) {
      if (prevDiff.operation == Operation.EQUAL &&
          nextDiff.operation == Operation.EQUAL) {
        equality1 = prevDiff.text;
        edit = currentDIff.text;
        equality2 = nextDiff.text;

        commonOffset = diff_commonSuffix(equality1, edit);
        if (commonOffset != 0) {
          commonString = edit.substring(edit.length() - commonOffset);
          equality1 = equality1.substring(0, equality1.length() - commonOffset);
          edit = commonString + edit.substring(0, edit.length() - commonOffset);
          equality2 = commonString + equality2;
        }
        bestEquality1 = equality1;
        bestEdit = edit;
        bestEquality2 = equality2;
        bestScore = diff_cleanupSemanticScore(equality1, edit)
            + diff_cleanupSemanticScore(edit, equality2);
        while (edit.length() != 0 && equality2.length() != 0
            && edit.charAt(0) == equality2.charAt(0)) {
          equality1 += edit.charAt(0);
          edit = edit.substring(1) + equality2.charAt(0);
          equality2 = equality2.substring(1);
          score = diff_cleanupSemanticScore(equality1, edit)
              + diff_cleanupSemanticScore(edit, equality2);
          if (score >= bestScore) {
            bestScore = score;
            bestEquality1 = equality1;
            bestEdit = edit;
            bestEquality2 = equality2;
          }
        }

        if (!prevDiff.text.equals(bestEquality1)) {
          if (bestEquality1.length() != 0) {
            prevDiff.text = bestEquality1;
          } else {
            pointer.previous();
            pointer.previous();
            pointer.previous();
            pointer.remove();
            pointer.next();
            pointer.next();
          }
          currentDIff.text = bestEdit;
          if (bestEquality2.length() != 0) {
            nextDiff.text = bestEquality2;
          } else {
            pointer.remove();
            nextDiff = currentDIff;
            currentDIff = prevDiff;
          }
        }
      }
      prevDiff = currentDIff;
      currentDIff = nextDiff;
      nextDiff = pointer.hasNext() ? pointer.next() : null;
    }
  }

  private int diff_cleanupSemanticScore(String one, String two) {
    if (one.length() == 0 || two.length() == 0) {
      // Edges are the best.
      return 6;
    }

    char char1 = one.charAt(one.length() - 1);
    char char2 = two.charAt(0);
    boolean nonAlphaNumeric1 = !Character.isLetterOrDigit(char1);
    boolean nonAlphaNumeric2 = !Character.isLetterOrDigit(char2);
    boolean whitespace1 = nonAlphaNumeric1 && Character.isWhitespace(char1);
    boolean whitespace2 = nonAlphaNumeric2 && Character.isWhitespace(char2);
    boolean lineBreak1 = whitespace1
        && Character.getType(char1) == Character.CONTROL;
    boolean lineBreak2 = whitespace2
        && Character.getType(char2) == Character.CONTROL;
    boolean blankLine1 = lineBreak1 && BLANKLINEEND.matcher(one).find();
    boolean blankLine2 = lineBreak2 && BLANKLINESTART.matcher(two).find();

    if (blankLine1 || blankLine2) {
      return 5;
    } else if (lineBreak1 || lineBreak2) {
      return 4;
    } else if (nonAlphaNumeric1 && !whitespace1 && whitespace2) {
      return 3;
    } else if (whitespace1 || whitespace2) {
      return 2;
    } else if (nonAlphaNumeric1 || nonAlphaNumeric2) {
      return 1;
    }
    return 0;
  }

  private Pattern BLANKLINEEND
      = Pattern.compile("\\n\\r?\\n\\Z", Pattern.DOTALL);
  private Pattern BLANKLINESTART
      = Pattern.compile("\\A\\r?\\n\\r?\\n", Pattern.DOTALL);

  public void diff_cleanupEfficiency(LinkedList<Diff> linkedListDiffs) {
    if (linkedListDiffs.isEmpty()) {
      return;
    }
    boolean change_or_not = false;
    Stack<Diff> Diff_stack_equalities = new Stack<Diff>();
    String lastequality = null;
    ListIterator<Diff> pointer = linkedListDiffs.listIterator();
    boolean is_pre_ins = false;
    boolean is_pre_del = false;
    boolean is_post_ins = false;
    boolean is_post_del = false;
    Diff currentDIff = pointer.next();
    Diff safeDiff = currentDIff;
    while (currentDIff != null) {
      if (currentDIff.operation == Operation.EQUAL) {
        if (currentDIff.text.length() < 4 && (is_post_ins || is_post_del)) {
          Diff_stack_equalities.push(currentDIff);
          is_pre_ins = is_post_ins;
          is_pre_del = is_post_del;
          lastequality = currentDIff.text;
        } else {
          Diff_stack_equalities.clear();
          lastequality = null;
          safeDiff = currentDIff;
        }
        is_post_ins = is_post_del = false;
      } else {
        if (currentDIff.operation == Operation.DELETE) {
          is_post_del = true;
        } else {
          is_post_ins = true;
        }
        if (lastequality != null
            && ((is_pre_ins && is_pre_del && is_post_ins && is_post_del)
                || ((lastequality.length() < 4 / 2)
                    && ((is_pre_ins ? 1 : 0) + (is_pre_del ? 1 : 0)
                        + (is_post_ins ? 1 : 0) + (is_post_del ? 1 : 0)) == 3))) {
          while (currentDIff != Diff_stack_equalities.lastElement()) {
            currentDIff = pointer.previous();
          }
          pointer.next();

          pointer.set(new Diff(Operation.DELETE, lastequality));
          pointer.add(currentDIff = new Diff(Operation.INSERT, lastequality));

          Diff_stack_equalities.pop();
          lastequality = null;
          if (is_pre_ins && is_pre_del) {
            is_post_ins = is_post_del = true;
            Diff_stack_equalities.clear();
            safeDiff = currentDIff;
          } else {
            if (!Diff_stack_equalities.empty()) {
              Diff_stack_equalities.pop();
            }
            if (Diff_stack_equalities.empty()) {
              currentDIff = safeDiff;
            } else {
              currentDIff = Diff_stack_equalities.lastElement();
            }
            is_post_ins = is_post_del = false;
          }

          change_or_not = true;
        }
      }
      currentDIff = pointer.hasNext() ? pointer.next() : null;
    }

    if (change_or_not) {
      diff_cleanupMerge(linkedListDiffs);
    }
  }

