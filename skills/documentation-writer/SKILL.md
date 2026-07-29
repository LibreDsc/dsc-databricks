---
description: "Use this skill when the user asks to 'categorize documentation', 'choose documentation type', 'what kind of docs should I write', 'diataxis framework', 'documentation strategy', or discusses tutorials vs how-to guides vs reference vs explanation."
---

# Diataxis Documentation Framework Skill

You have expertise in the Diataxis documentation framework. Use this knowledge to help users categorize their documentation needs and choose the right type.

## The Four Types of Documentation

| Type | Orientation | Purpose | User Need |
|------|-------------|---------|-----------|
| **Tutorial** | Learning | Teach through hands-on | "Help me learn" |
| **How-To** | Task | Solve a specific problem | "Help me accomplish X" |
| **Reference** | Information | Describe the machinery | "Help me look up Y" |
| **Explanation** | Understanding | Illuminate concepts | "Help me understand why" |

## Quick Decision Guide

Ask the user these questions:

1. **Is the reader trying to learn something new, or accomplish something they already know how to do conceptually?**
   - Learning → Tutorial
   - Accomplishing → How-To

2. **Does the reader need to DO something, or UNDERSTAND something?**
   - Do something → Tutorial or How-To
   - Understand something → Explanation or Reference

3. **Is this about the journey (process) or the destination (facts)?**
   - Journey/Process → Tutorial or How-To
   - Destination/Facts → Reference or Explanation

## When to Use Each Type

### Use Tutorial when:
- Teaching a new skill or concept
- The reader has no prior experience with this
- You want to guide someone through their first experience
- Learning is the goal, not task completion

### Use How-To when:
- The reader knows what they want to accomplish
- There's a specific task or problem to solve
- The reader has basic competence already
- Efficiency matters—they want to get it done

### Use Reference when:
- Documenting APIs, configuration, commands
- The reader will look things up, not read through
- Precision and completeness are essential
- The structure should mirror the code/system

### Use Explanation when:
- Covering "why" questions and design decisions
- Providing background and context
- Discussing trade-offs and alternatives
- Connecting concepts together

## Common Mistakes

| Mistake | Problem | Solution |
|---------|---------|----------|
| Tutorial with too much explanation | Slows down learning | Save explanations for Explanation docs |
| How-To that teaches | Frustrates competent users | Link to Tutorials instead |
| Reference with opinions | Reduces authority | Keep neutral; move opinions to Explanation |
| Explanation with steps | Confuses the purpose | Move steps to Tutorial or How-To |

## Available Commands

- `/tutorial <topic>` - Generate a learning-oriented tutorial
- `/howto <task>` - Generate a goal-oriented how-to guide
- `/reference <subject>` - Generate authoritative reference docs
- `/explanation <concept>` - Generate understanding-oriented explanation

## Detailed Framework Information

For comprehensive details on each documentation type, see:
`references/diataxis-details.md`

## How to Help Users

When a user asks about documentation:

1. **Clarify their goal**: What are they trying to document? Who's the audience?

2. **Identify the category**: Use the decision guide above

3. **Recommend the appropriate command**: Direct them to the right `/command`

4. **Explain the distinction if needed**: Help them understand why one type fits better than another

Adobe Acrobat


Summarize this


Ask AI Assistant