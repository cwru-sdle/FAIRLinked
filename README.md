------------------
FAIRLinked
------------------

**Authors:**  
**Balashanmuga Priyan Rajamohan**, Kai Zheng, Benjamin Pierce, Yinghui Wu, Laura Bruckman, Erika I. Barcelos, Roger H. French

**Affiliation:**  
Materials Data Science for Stockpile Stewardship Center of Excellence,  
Cleveland, OH 44106, USA


**Python Installation:**

```bash
pip install FAIRLinked
```

**Package Usage:**

```bash
FAIRLinked
```

```mermaid
  graph TD;
    A[User Input] --> B{Existing Data Cube File?};
    B -->|Yes| C[Parse RDF to DataFrame];
    B -->|No| D{Running Experiment?};
    D -->|Yes| E[Experiment Workflow];
    D -->|No| F[Standard Workflow];

    C --> G{Save as CSV?};
    G -->|Yes| H[Save CSV];
    G -->|No| I[End];

    E --> J{Has Ontology Files?};
    J -->|Yes| K[Analyze Ontologies];
    J -->|No| L[Generate Default Templates];
    K --> M[Generate Templates];
    L --> M;
    M --> N[Create Namespace Excel];
    M --> O[Create Data Excel];

    F --> P[Process Input Files];
    P --> Q[Parse Namespace];
    P --> R[Read Data Template];
    Q --> S[Convert to RDF];
    R --> S;
    S --> T[Output Files];
```


# Acknowledgment:
This work was supported by the U.S. Department of Energy’s Office of Energy Efficiency and Renewable Energy (EERE) under the Solar Energy Technologies Office (SETO) Agreement Numbers DE-EE0009353 and DE-EE0009347; the Department of Energy (National Nuclear Security Administration) under Award Number DE-NA0004104 and Contract Number B647887; and the U.S. National Science Foundation under Award Number 2133576.