# Diataxis Framework: Complete Reference

This document provides comprehensive details on the Diataxis documentation framework, created by Daniele Procida.

## Framework Overview

Diataxis identifies four distinct types of documentation, each serving different user needs and requiring different approaches:

```
                    PRACTICAL                    THEORETICAL
                 (doing/action)              (understanding/cognition)
              ┌─────────────────────┬─────────────────────┐
              │                     │                     │
   LEARNING   │     TUTORIALS       │    EXPLANATION      │
   (study)    │                     │                     │
              │   Learning-oriented │ Understanding-      │
              │   Hands-on lessons  │ oriented            │
              │                     │ Conceptual          │
              │                     │ discussion          │
              ├─────────────────────┼─────────────────────┤
              │                     │                     │
   WORKING    │    HOW-TO GUIDES    │    REFERENCE        │
   (apply)    │                     │                     │
              │   Task-oriented     │ Information-        │
              │   Problem-solving   │ oriented            │
              │   directions        │ Technical           │
              │                     │ description         │
              └─────────────────────┴─────────────────────┘
```

---

## Tutorials: Learning-Oriented

### Purpose
Tutorials are **lessons** that take the reader by the hand through a series of steps to complete a project. They exist to help newcomers get started and develop basic competence.

### Key Characteristics

**The Teacher Analogy**
Think of a tutorial like teaching a child to cook:
- You are responsible for what the learner does
- The learner is there to learn, not to accomplish
- You provide materials and instructions
- Success means the learner has acquired skills

**Essential Requirements**

1. **Learning, not accomplishing**: The goal is skill acquisition, not task completion
2. **Getting started**: Helps users begin their journey with confidence
3. **Ensure success**: Every step must work; failure destroys confidence
4. **Visible results**: Frequent, tangible outcomes keep learners engaged
5. **Concrete, not abstract**: Use specific examples, not general principles
6. **Minimum explanation**: Learners acquire understanding through doing
7. **Ignore alternatives**: Make decisions for the learner; don't offer choices
8. **Enable, don't teach**: Provide experiences that allow discovery; don't lecture
9. **Embrace repetition**: Design steps that can be repeated for reinforcement

**Language Patterns**
- "In this tutorial, we will..."
- "First, create..."
- "You should see..."
- "The output should look like..."

**Structure**
```markdown
# Tutorial: [What They'll Learn]

Introduction: What we'll accomplish

## Prerequisites
- Specific requirements

## Step 1: [Action]
Do this specific thing.
[code/command]
You should see: [expected result]

## Step 2: [Action]
...

## What We Learned
Summary of skills acquired

## Next Steps
Where to go from here
```

### Common Mistakes
- Too much explanation before doing
- Offering choices or alternatives
- Steps that might fail
- Assuming prior knowledge
- Not confirming expected results

---

## How-To Guides: Task-Oriented

### Purpose
How-to guides are **directions** that take the reader through steps to solve a real-world problem. They are goal-oriented and assume competence.

### Key Characteristics

**The Recipe Analogy**
Think of a how-to guide like a cooking recipe:
- Assumes you know how to cook
- Addresses a specific outcome
- Lists what you need and steps to follow
- Allows for variations and substitutions

**Essential Requirements**

1. **Problem-oriented**: Defined by the user's need, not the tool
2. **Assume competence**: The reader knows what they want to do
3. **Action focus**: Steps, not concepts
4. **Flexibility**: Offer variations for different situations
5. **Practical, not teaching**: Link to tutorials for learning
6. **Well-named**: Titles should state the task: "How to X"
7. **Usability over completeness**: Omit the unnecessary; practical value trumps exhaustiveness

**Language Patterns**
- "If you want X, do Y"
- "To accomplish X..."
- "Alternatively, you can..."
- Problem/solution framing

**Structure**
```markdown
# How to [Accomplish Task]

Brief statement of what this achieves

## Prerequisites
- What must be in place

## Steps

### 1. [Action]
[Concise instruction]

### 2. [Action]
If [condition], do [alternative] instead.

## Variations
### [Different scenario]
Modified approach for this case

## Troubleshooting
Common issues and solutions

## Related
Links to tutorials, reference, explanation
```

### Common Mistakes
- Teaching concepts instead of showing steps
- Too much explanation
- Not acknowledging variations
- Assuming the reader doesn't know their goal
- Over-specifying (not allowing flexibility)

---

## Reference: Information-Oriented

### Purpose
Reference documentation is **technical description** of the machinery and how to operate it. It is information-oriented and structured for lookup, not learning.

### Key Characteristics

**The Dictionary/Encyclopedia Analogy**
Think of reference docs like a dictionary or encyclopedia:
- Organized by structure, not narrative
- Each entry is self-contained
- Neutral, authoritative tone
- Designed for lookup, not reading through

**Essential Requirements**

1. **Structure mirrors the product**: Organize like the code itself
2. **Consistency**: Same format for same types of things
3. **Accurate**: Factually correct, up to date
4. **Precision**: No ambiguity; use exact technical terms
5. **Complete**: All public interfaces documented
6. **Neutral**: Describe, don't recommend or explain
7. **Austere**: No unnecessary words or friendliness
8. **Include warnings**: Document limitations, edge cases, and error conditions

**Language Patterns**
- Declarative statements
- Technical terminology
- "Returns...", "Accepts...", "Raises..."
- No "you" or conversational language

**Structure**
```markdown
# [Component] Reference

## `function_name(params)`

Brief description.

### Parameters
| Name | Type | Description |
|------|------|-------------|

### Returns
Type and description

### Raises
Exceptions and conditions

### Example
```code
Concise usage example
```

## `another_function()`
...
```

### Common Mistakes
- Including tutorials within reference
- Explaining why things work (that's explanation)
- Inconsistent formatting
- Missing entries
- Conversational or friendly tone
- Opinions or recommendations

---

## Explanation: Understanding-Oriented

### Purpose
Explanation is **discussion** that clarifies, illuminates, and provides context. It is understanding-oriented and helps readers grasp the bigger picture.

### Key Characteristics

**The "About" Model**
Explanation titles often work as "About [topic]":
- About authentication strategies
- About the event system architecture
- About choosing between SQL and NoSQL

**Essential Requirements**

1. **Context**: Historical background, evolution, constraints
2. **Connections**: Link ideas together, show relationships
3. **Alternatives**: Acknowledge other approaches fairly
4. **Trade-offs**: Discuss what we gain and sacrifice
5. **Multiple perspectives**: Present different viewpoints
6. **No steps**: This is discussion, not instruction
7. **Higher perspective**: Step back from details
8. **Admit opinions**: It's appropriate to express preferences with reasoning
9. **Bounded scope**: Use a "why" question to establish clear topic boundaries

**Language Patterns**
- "The reason for X is..."
- "This approach trades off A for B..."
- "Unlike X, Y provides..."
- "Historically, this evolved from..."
- "Consider this from the perspective of..."

**Structure**
```markdown
# About [Topic]

Introduction framing the concept

## Background
Historical context and evolution

## How [Topic] Works
Conceptual (not step-by-step) explanation

## Why [Topic]
Design decisions and reasoning

## Trade-offs
### Advantages
### Limitations

## Alternatives
Other approaches and when to use them

## Related Concepts
Connections to other ideas

## Further Reading
Links to tutorials, how-to, reference
```

### Common Mistakes
- Including step-by-step instructions
- Not acknowledging trade-offs
- Presenting opinion as fact
- Missing the bigger picture
- Not connecting to other concepts

---

## Relationships Between Types

### How They Connect

```
Tutorial → "Now that you've learned, see How-To for tasks"
How-To → "Need to learn first? See Tutorial"
         "For all options, see Reference"
         "To understand why, see Explanation"
Reference → "For conceptual background, see Explanation"
Explanation → "To apply this, see How-To"
              "For precise details, see Reference"
```

### Keeping Them Separate

Each type should link to others rather than incorporate their content:

| Instead of... | Do this... |
|---------------|------------|
| Explaining in a tutorial | Link to explanation |
| Teaching in a how-to | Link to tutorial |
| Recommending in reference | Link to how-to |
| Step-by-step in explanation | Link to how-to |

---

## Quality Checklist

### Tutorial
- [ ] Focuses on learning, not accomplishing
- [ ] Every step produces visible results
- [ ] No choices or alternatives offered
- [ ] Minimal explanation
- [ ] Every step is guaranteed to work
- [ ] Confirms what learner should see

### How-To
- [ ] States the task/goal clearly
- [ ] Assumes user competence
- [ ] Concise, action-focused steps
- [ ] Offers variations where appropriate
- [ ] Links to learning resources, doesn't embed

### Reference
- [ ] Structure mirrors the product
- [ ] Consistent format throughout
- [ ] Complete coverage
- [ ] Neutral, authoritative tone
- [ ] Concise examples (not tutorials)

### Explanation
- [ ] Provides context and background
- [ ] Discusses trade-offs fairly
- [ ] Acknowledges alternatives
- [ ] No step-by-step instructions
- [ ] Connects concepts together

---

## Attribution

The Diataxis framework was created by Daniele Procida. For the authoritative source, see: https://diataxis.fr/