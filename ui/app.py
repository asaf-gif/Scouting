import streamlit as st
import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

st.set_page_config(
    page_title="Systematic Problem Scouting",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def run_query(cypher: str, **params):
    driver = get_driver()
    with driver.session() as s:
        return s.run(cypher, **params).data()


# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.title("Problem Scouting")
st.sidebar.caption("Multi-agent disruption tracker")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    ["📚 BM Library", "🔀 Transition Case Studies", "📐 Transformations",
     "⚡ Scalars", "🔬 Technologies", "🧠 Hypotheses", "🏢 Companies",
     "📋 Input Review Queue", "📊 Graph Overview",
     "🔬 Validation Review", "📝 Editorial Queue", "🔄 Pipeline Monitor"],
)

st.sidebar.divider()
st.sidebar.markdown("""
**Build status**

| Part | Status |
|------|--------|
| 1–4 Infrastructure | ✅ |
| 5 Manual BM Entry | ✅ |
| 6 Company Upload | ✅ |
| 7 Technology Entry | ✅ |
| 8 Internet Scan | ✅ |
| 9 Input Review UI | ✅ |
| 10–13 Extraction | ✅ |
| 14–17 Analysis | ✅ |
| 18–21 Research | ✅ |
| 22–23 Monitor | ✅ |
| 24–25 Editorial | ✅ |
| 26–28 Orchestration | ✅ |
""")


# ── Page: Graph Overview ──────────────────────────────────────────────────────

# ── Page: BM Library ─────────────────────────────────────────────────────────

if page == "📚 BM Library":
    st.title("📚 Business Model Library")
    st.caption("Review, edit and annotate all business models. Every change is logged with your reason and used to calibrate extraction.")

    # ── Load all BMs ──────────────────────────────────────────────────────────
    bms = run_query("""
        MATCH (b:BusinessModel)
        OPTIONAL MATCH (b)<-[:FROM_BIM]-(v:TransformationVector)
        OPTIONAL MATCH (e:Evidence)-[:SUPPORTS]->(v)
        WITH b,
             count(DISTINCT v) AS vector_count,
             count(DISTINCT e) AS evidence_count
        RETURN b.bim_id        AS id,
               b.name          AS name,
               b.description   AS description,
               b.source        AS source,
               b.examples      AS examples,
               b.status        AS status,
               b.pending_human_review AS pending_review,
               b.added_by      AS added_by,
               b.created_at    AS created_at,
               vector_count,
               evidence_count
        ORDER BY b.bim_id
    """)

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns([3, 2, 2])
    with col_f1:
        search = st.text_input("🔍 Search", placeholder="Filter by name or description...")
    with col_f2:
        show_pending = st.checkbox("Show pending review only", value=False)
    with col_f3:
        sort_by = st.selectbox("Sort by", ["ID", "Name", "Evidence count", "Vector count"])

    # Filter
    filtered = bms
    if search:
        q = search.lower()
        filtered = [b for b in filtered if q in (b["name"] or "").lower()
                    or q in (b["description"] or "").lower()]
    if show_pending:
        filtered = [b for b in filtered if b.get("pending_review")]

    # Sort
    if sort_by == "Name":
        filtered = sorted(filtered, key=lambda x: x["name"] or "")
    elif sort_by == "Evidence count":
        filtered = sorted(filtered, key=lambda x: x["evidence_count"] or 0, reverse=True)
    elif sort_by == "Vector count":
        filtered = sorted(filtered, key=lambda x: x["vector_count"] or 0, reverse=True)

    st.divider()
    st.caption(f"Showing {len(filtered)} of {len(bms)} business models")

    # ── Change log loader ─────────────────────────────────────────────────────
    changelog_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bm_changelog.jsonl"
    )

    def append_changelog(entry: dict):
        os.makedirs(os.path.dirname(changelog_path), exist_ok=True)
        with open(changelog_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def load_changelog(bim_id: str) -> list:
        if not os.path.exists(changelog_path):
            return []
        entries = []
        with open(changelog_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("bim_id") == bim_id:
                        entries.append(e)
                except Exception:
                    pass
        return entries

    # ── Render each BM ────────────────────────────────────────────────────────
    for bm in filtered:
        bim_id  = bm["id"]
        name    = bm["name"] or bim_id
        pending = bm.get("pending_review")
        status  = bm.get("status", "Active") or "Active"

        label = f"{'🔴 ' if pending else ''}{bim_id}: {name}"
        with st.expander(label, expanded=False):

            # ── Top row: clickable stat buttons ───────────────────────────────
            vec_key = f"show_vectors_{bim_id}"
            evd_key = f"show_evidence_{bim_id}"
            if vec_key not in st.session_state:
                st.session_state[vec_key] = False
            if evd_key not in st.session_state:
                st.session_state[evd_key] = False

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                vec_count = bm["vector_count"] or 0
                vec_label = f"🔗 {vec_count} Transitions FROM"
                if st.button(vec_label, key=f"btn_vec_{bim_id}",
                             help="Click to see all recorded transition paths from this model",
                             use_container_width=True):
                    st.session_state[vec_key] = not st.session_state[vec_key]
            with m2:
                evd_count = bm["evidence_count"] or 0
                evd_label = f"📄 {evd_count} Evidence nodes"
                if st.button(evd_label, key=f"btn_evd_{bim_id}",
                             help="Click to see all real-world examples linked to this model",
                             use_container_width=True):
                    st.session_state[evd_key] = not st.session_state[evd_key]
            with m3:
                st.metric("Status", status)
            with m4:
                st.metric("Added by", bm.get("added_by") or "system")

            # ── Vectors detail panel ──────────────────────────────────────────
            if st.session_state[vec_key]:
                st.markdown("---")
                st.markdown(f"### 🔗 All transitions FROM **{name}**")
                all_vectors = run_query("""
                    MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel {bim_id: $bid})
                    MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                    WITH v, t, size([(e:Evidence)-[:SUPPORTS]->(v)|e]) AS ev_count
                    OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
                    WITH v, t, ev_count, head(collect(h)) AS h
                    RETURN v.vector_id       AS vector_id,
                           t.name            AS to_name,
                           t.bim_id          AS to_id,
                           v.signal_strength AS signal,
                           ev_count,
                           h.hypothesis_id   AS hyp_id,
                           h.conviction_score AS conviction,
                           h.title           AS hyp_title
                    ORDER BY coalesce(v.signal_strength, 0) DESC
                """, bid=bim_id)

                if not all_vectors:
                    st.info("No transitions recorded from this model yet.")
                else:
                    st.caption(f"{len(all_vectors)} transition paths recorded")
                    for vrow in all_vectors:
                        sig   = vrow.get("signal") or 0
                        ev_n  = vrow.get("ev_count") or 0
                        sig_bar = "█" * int(sig * 10) + "░" * (10 - int(sig * 10))

                        with st.container(border=True):
                            c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                            c1.markdown(f"**→ {vrow['to_name']}**")
                            c1.caption(f"`{vrow['vector_id']}`")
                            c2.markdown(f"Signal: `{sig:.3f}`")
                            c2.caption(sig_bar)
                            c3.caption(f"{ev_n} evidence node{'s' if ev_n != 1 else ''}")
                            if vrow.get("hyp_id"):
                                c4.markdown(f"💡 Hypothesis")
                                c4.caption(f"Conviction: {vrow.get('conviction') or 0:.2f}")
                                if vrow.get("hyp_title"):
                                    st.caption(f"*\"{vrow['hyp_title'][:100]}\"*")
                            else:
                                c4.caption("No hypothesis yet")

                            # Show evidence quotes for this vector
                            ev_rows = run_query("""
                                MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector {vector_id: $vid})
                                RETURN e.evidence_quote       AS quote,
                                       e.transition_summary   AS summary,
                                       e.companies_mentioned  AS companies,
                                       e.source_url           AS url,
                                       e.source_type          AS stype,
                                       e.confidence           AS conf,
                                       e.created_at           AS created
                                ORDER BY e.confidence DESC
                            """, vid=vrow["vector_id"])
                            if ev_rows:
                                eq_key = f"show_eq_{vrow['vector_id']}"
                                if eq_key not in st.session_state:
                                    st.session_state[eq_key] = False
                                if st.button(f"📄 {len(ev_rows)} evidence quote{'s' if len(ev_rows)>1 else ''}",
                                             key=f"btn_eq_{vrow['vector_id']}"):
                                    st.session_state[eq_key] = not st.session_state[eq_key]
                                if st.session_state[eq_key]:
                                    for er in ev_rows:
                                        companies = er.get("companies") or []
                                        if isinstance(companies, str):
                                            try:
                                                import json as _json
                                                companies = _json.loads(companies)
                                            except Exception:
                                                companies = [companies]
                                        company_str = ", ".join(companies) if companies else "unknown company"
                                        st.markdown(f"**🏢 {company_str}**")
                                        summary = er.get("summary") or ""
                                        if summary:
                                            st.markdown(f"_{summary}_")
                                        quote = er.get("quote") or ""
                                        if quote and quote != summary:
                                            st.markdown(f"> \"{quote}\"")
                                        src = er.get("url") or ""
                                        if src:
                                            st.caption(f"conf={er.get('conf') or 0:.2f}  ·  [{src[:60]}]({src})")
                                        else:
                                            st.caption(f"conf={er.get('conf') or 0:.2f}")
                                        st.divider()

            # ── Evidence detail panel ─────────────────────────────────────────
            if st.session_state[evd_key]:
                st.markdown("---")
                st.markdown(f"### 📄 All evidence linked to **{name}**")
                all_evidence = run_query("""
                    MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel {bim_id: $bid})
                    MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                    RETURN e.evidence_id          AS evd_id,
                           e.evidence_quote        AS quote,
                           e.transition_summary    AS summary,
                           e.companies_mentioned   AS companies,
                           e.source_url            AS url,
                           e.source_type           AS stype,
                           e.confidence            AS conf,
                           e.created_at            AS created,
                           t.name                  AS to_name,
                           v.vector_id             AS vector_id
                    ORDER BY e.confidence DESC
                """, bid=bim_id)

                if not all_evidence:
                    st.info("No evidence recorded yet for transitions from this model.")
                else:
                    st.caption(f"{len(all_evidence)} real-world examples found")
                    for er in all_evidence:
                        with st.container(border=True):
                            src = er.get("url") or ""
                            conf = er.get("conf") or 0
                            companies = er.get("companies") or []
                            if isinstance(companies, str):
                                try:
                                    import json as _json
                                    companies = _json.loads(companies)
                                except Exception:
                                    companies = [companies]
                            company_str = ", ".join(companies) if companies else "unknown company"

                            # Header row: company → destination
                            st.markdown(f"**🏢 {company_str}** &nbsp;→&nbsp; **{er.get('to_name', '?')}**")

                            # Summary (cleaner than raw quote)
                            summary = er.get("summary") or ""
                            if summary:
                                st.markdown(f"_{summary}_")

                            # Raw quote (secondary)
                            quote = er.get("quote") or ""
                            if quote and quote != summary:
                                st.markdown(f"> \"{quote}\"")

                            # Footer: confidence + source
                            footer_parts = [f"conf={conf:.2f}", er.get('stype') or ""]
                            if src:
                                st.caption(f"{' · '.join(p for p in footer_parts if p)}  🔗 [{src[:70]}]({src})")
                            else:
                                st.caption(" · ".join(p for p in footer_parts if p))

            st.divider()

            # ── Description ───────────────────────────────────────────────────
            st.markdown("**Description**")
            current_desc = bm["description"] or ""
            st.markdown(f"> {current_desc}" if current_desc else "_No description_")

            # ── Source + Examples ─────────────────────────────────────────────
            col_s, col_e = st.columns(2)
            with col_s:
                st.markdown("**Source**")
                st.caption(bm.get("source") or "_not set_")
            with col_e:
                st.markdown("**Company examples**")
                examples = bm.get("examples") or ""
                st.caption(examples if examples else "_none recorded_")

            st.divider()

            # ── Edit section ──────────────────────────────────────────────────
            with st.form(key=f"edit_{bim_id}"):
                st.markdown("**✏️ Edit this Business Model**")

                new_name = st.text_input("Name", value=name)
                new_desc = st.text_area("Description", value=current_desc, height=150,
                                        help="Full description used by the extraction LLM to classify companies")
                new_examples = st.text_input("Company examples (comma-separated)",
                                             value=examples,
                                             help="e.g. Netflix, Spotify, Adobe")
                new_source = st.text_input("Source / reference",
                                           value=bm.get("source") or "")
                change_reason = st.text_area(
                    "🧠 Why are you making this change?",
                    height=80,
                    placeholder="e.g. 'Description was too narrow — SaaS is being confused with generic subscription models. Adding clearer boundary around software delivery.'",
                    help="This is stored in the changelog and fed back to improve extraction prompts"
                )

                col_b1, col_b2 = st.columns([2, 1])
                with col_b1:
                    submitted = st.form_submit_button("💾 Save changes", type="primary")
                with col_b2:
                    mark_reviewed = st.form_submit_button("✅ Mark as reviewed")

                if submitted:
                    if not change_reason.strip():
                        st.error("Please provide a reason for the change — this is used to improve extraction.")
                    else:
                        now = datetime.now(timezone.utc).isoformat()
                        run_query("""
                            MATCH (b:BusinessModel {bim_id: $bid})
                            SET b.name        = $name,
                                b.description = $desc,
                                b.examples    = $examples,
                                b.source      = $source,
                                b.last_edited_at = $now,
                                b.last_edited_by = 'editorial'
                        """, bid=bim_id, name=new_name, desc=new_desc,
                             examples=new_examples, source=new_source, now=now)

                        append_changelog({
                            "bim_id":      bim_id,
                            "name":        name,
                            "timestamp":   now,
                            "editor":      "editorial",
                            "field":       "description+name+examples",
                            "old_desc":    current_desc,
                            "new_desc":    new_desc,
                            "old_name":    name,
                            "new_name":    new_name,
                            "reason":      change_reason.strip(),
                        })
                        st.success(f"✅ {bim_id} updated and change logged.")
                        st.rerun()

                if mark_reviewed:
                    run_query("""
                        MATCH (b:BusinessModel {bim_id: $bid})
                        SET b.pending_human_review = false,
                            b.reviewed_at = $now
                    """, bid=bim_id, now=datetime.now(timezone.utc).isoformat())
                    st.success("Marked as reviewed.")
                    st.rerun()

            # ── Change history ────────────────────────────────────────────────
            history = load_changelog(bim_id)
            if history:
                hist_key = f"show_hist_{bim_id}"
                if hist_key not in st.session_state:
                    st.session_state[hist_key] = False
                if st.button(f"📋 Change history ({len(history)} edit{'s' if len(history)>1 else ''})",
                             key=f"btn_hist_{bim_id}"):
                    st.session_state[hist_key] = not st.session_state[hist_key]
                if st.session_state[hist_key]:
                    for entry in reversed(history):
                        st.markdown(f"**{entry.get('timestamp','')[:19]}**")
                        st.markdown(f"*Reason:* {entry.get('reason','')}")
                        if entry.get('old_desc') != entry.get('new_desc'):
                            st.markdown(f"*Description changed*")
                        if entry.get('old_name') != entry.get('new_name'):
                            st.markdown(f"*Renamed:* `{entry.get('old_name')}` → `{entry.get('new_name')}`")
                        st.divider()

    # ── Global changelog export ───────────────────────────────────────────────
    st.divider()
    st.subheader("📋 Full Change Log")
    if os.path.exists(changelog_path):
        with open(changelog_path) as f:
            all_entries = [json.loads(l) for l in f if l.strip()]
        if all_entries:
            st.caption(f"{len(all_entries)} total edits logged")
            st.dataframe(
                [{"Time": e.get("timestamp","")[:19],
                  "BM": e.get("bim_id",""),
                  "Name": e.get("new_name",""),
                  "Reason": e.get("reason","")[:80]}
                 for e in reversed(all_entries)],
                use_container_width=True, hide_index=True
            )
            if st.button("📥 Download changelog as JSON"):
                st.download_button(
                    "Download", data=open(changelog_path).read(),
                    file_name="bm_changelog.jsonl", mime="application/json"
                )
    else:
        st.caption("No changes logged yet.")


# ── Page: Transition Case Studies ────────────────────────────────────────────

elif page == "🔀 Transition Case Studies":
    st.title("🔀 Transition Case Studies")
    st.caption("Every recorded real-world business model transition — the companies, the story, the causal forces. Edit any case and log your reason.")

    # ── Changelog helpers ─────────────────────────────────────────────────────
    tc_log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "transition_changelog.jsonl"
    )

    def tc_append(entry: dict):
        os.makedirs(os.path.dirname(tc_log_path), exist_ok=True)
        with open(tc_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def tc_load(evidence_id: str) -> list:
        if not os.path.exists(tc_log_path):
            return []
        out = []
        with open(tc_log_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("evidence_id") == evidence_id:
                        out.append(e)
                except Exception:
                    pass
        return out

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3, col_f4 = st.columns([3, 2, 2, 2])
    with col_f1:
        tc_search = st.text_input("🔍 Search companies or summary", key="tc_search",
                                   placeholder="e.g. Netflix, Adobe, Spotify...")
    with col_f2:
        # Get distinct from-BMs for filter
        from_bms = run_query("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
            RETURN DISTINCT f.name AS name ORDER BY f.name
        """)
        from_options = ["All"] + [r["name"] for r in from_bms]
        tc_from = st.selectbox("From BM", from_options, key="tc_from")
    with col_f3:
        to_bms = run_query("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:TO_BIM]->(t:BusinessModel)
            RETURN DISTINCT t.name AS name ORDER BY t.name
        """)
        to_options = ["All"] + [r["name"] for r in to_bms]
        tc_to = st.selectbox("To BM", to_options, key="tc_to")
    with col_f4:
        tc_sort = st.selectbox("Sort by", ["Signal strength", "Confidence", "Newest first"],
                                key="tc_sort")

    # ── Load evidence nodes (one per company story) ───────────────────────────
    sort_clause = {
        "Signal strength": "ORDER BY v.signal_strength DESC, e.confidence DESC",
        "Confidence":      "ORDER BY e.confidence DESC",
        "Newest first":    "ORDER BY e.extracted_at DESC",
    }[tc_sort]

    from_filter = "" if tc_from == "All" else "AND f.name = $from_bm"
    to_filter   = "" if tc_to   == "All" else "AND t.name = $to_bm"

    cases = run_query(f"""
        MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
        MATCH (v)-[:TO_BIM]->(t:BusinessModel)
        WHERE 1=1 {from_filter} {to_filter}
        WITH e, v, f, t,
             size([(sc:Scalar)<-[:IMPACTS]-(v)|sc]) AS scalar_count
        OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
        WITH e, v, f, t, scalar_count, head(collect(h)) AS h
        RETURN e.evidence_id          AS eid,
               e.companies_mentioned  AS companies,
               e.transition_summary   AS summary,
               e.evidence_quote       AS quote,
               e.source_url           AS url,
               e.source_type          AS stype,
               e.confidence           AS conf,
               e.extracted_at         AS extracted_at,
               v.vector_id            AS vid,
               v.signal_strength      AS signal,
               f.name                 AS from_bm,
               f.bim_id               AS from_id,
               t.name                 AS to_bm,
               t.bim_id               AS to_id,
               scalar_count,
               h.hypothesis_id        AS hyp_id,
               h.title                AS hyp_title,
               h.conviction_score     AS conviction,
               h.thesis               AS thesis,
               h.counter_argument     AS counter,
               h.disruption_type      AS dtype,
               h.time_horizon         AS horizon,
               h.ai_disruption_link   AS ai_link
        {sort_clause}
    """, from_bm=tc_from, to_bm=tc_to)

    # Apply text search
    if tc_search:
        q = tc_search.lower()
        cases = [c for c in cases if
                 q in " ".join(c.get("companies") or []).lower()
                 or q in (c.get("summary") or "").lower()
                 or q in (c.get("quote") or "").lower()
                 or q in (c.get("from_bm") or "").lower()
                 or q in (c.get("to_bm") or "").lower()]

    st.divider()
    st.caption(f"{len(cases)} case studies")

    if not cases:
        st.info("No case studies match your filters.")
    else:
        DIRECTION_OPTIONS = ["increases", "neutral", "decreases"]
        STRENGTH_OPTIONS  = ["strong", "moderate", "weak"]
        IMPACT_SCORE_MAP  = {
            ("increases", "strong"):   2,
            ("increases", "moderate"): 1,
            ("neutral",   "strong"):   0,
            ("neutral",   "moderate"): 0,
            ("neutral",   "weak"):     0,
            ("decreases", "moderate"): -1,
            ("decreases", "strong"):   -2,
        }

        for case in cases:
            eid     = case.get("eid") or ""
            vid     = case.get("vid") or ""
            signal  = case.get("signal") or 0
            conf    = case.get("conf") or 0
            has_hyp = bool(case.get("hyp_id"))

            companies = case.get("companies") or []
            if isinstance(companies, str):
                try:    companies = json.loads(companies)
                except Exception: companies = [companies]
            company_str = ", ".join(companies) if companies else "Unknown company"

            transition_label = f"{case['from_bm']} → {case['to_bm']}"
            card_label = f"{transition_label}  ·  {company_str}"
            with st.expander(card_label, expanded=False):

                st.markdown(f"<u>**{transition_label}**</u>  —  {company_str}",
                            unsafe_allow_html=True)

                # ── Stat row ──────────────────────────────────────────────────
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Signal", f"{signal:.3f}")
                s2.metric("Confidence", f"{conf:.2f}")
                s3.metric("Scalars", case.get("scalar_count") or 0)
                s4.metric("Hypothesis", "✅ Yes" if has_hyp else "—")

                st.divider()

                # ── The Story ─────────────────────────────────────────────────
                st.markdown("#### 📖 The Story")
                summary = case.get("summary") or ""
                st.markdown(summary if summary else "_No summary available_")

                quote = case.get("quote") or ""
                if quote and quote.strip() != summary.strip():
                    st.markdown(f"> \"{quote}\"")

                src = case.get("url") or ""
                if src:
                    st.caption(f"🔗 Source: [{src[:80]}]({src})  ·  {case.get('stype') or ''}")

                st.divider()

                # ── Edit story toggle ──────────────────────────────────────────
                ev_edit_key = f"show_ev_edit_{vid}_{eid}"
                if ev_edit_key not in st.session_state:
                    st.session_state[ev_edit_key] = False
                if st.button("✏️ Edit story", key=f"btn_ev_edit_{vid}_{eid}"):
                    st.session_state[ev_edit_key] = not st.session_state[ev_edit_key]

                if st.session_state[ev_edit_key]:
                    with st.form(key=f"tc_edit_{vid}_{eid}"):
                        new_companies = st.text_input(
                            "Companies involved (comma-separated)", value=", ".join(companies)
                        )
                        new_summary = st.text_area(
                            "Transition story / summary", value=summary, height=100
                        )
                        flag_wrong = st.checkbox("🚩 Flag as incorrect transition (wrong From/To BM)")
                        correct_from = correct_to = ""
                        if flag_wrong:
                            correct_from = st.text_input("Correct 'From' BM", placeholder="e.g. Licensing")
                            correct_to   = st.text_input("Correct 'To' BM",   placeholder="e.g. SaaS")
                        edit_reason = st.text_area(
                            "🧠 Why are you making this change?", height=70,
                            placeholder="e.g. 'PayPal moved to Platform/Marketplace, not pure Subscription.'",
                        )
                        tc_submitted = st.form_submit_button("💾 Save", type="primary")
                        if tc_submitted:
                            if not edit_reason.strip():
                                st.error("Please explain the reason.")
                            else:
                                now = datetime.now(timezone.utc).isoformat()
                                new_co_list = [c.strip() for c in new_companies.split(",") if c.strip()]
                                run_query("""
                                    MATCH (e:Evidence {evidence_id: $eid})
                                    SET e.companies_mentioned = $companies,
                                        e.transition_summary  = $summary,
                                        e.last_edited_at      = $now,
                                        e.last_edited_by      = 'editorial'
                                """, eid=eid, companies=new_co_list, summary=new_summary, now=now)
                                tc_append({
                                    "evidence_id":   eid,
                                    "vector_id":     vid,
                                    "from_bm":       case["from_bm"],
                                    "to_bm":         case["to_bm"],
                                    "timestamp":     now,
                                    "change_type":   "evidence_edit",
                                    "old_companies": companies,
                                    "new_companies": new_co_list,
                                    "old_summary":   summary,
                                    "new_summary":   new_summary,
                                    "flagged_wrong": flag_wrong,
                                    "correct_from":  correct_from,
                                    "correct_to":    correct_to,
                                    "reason":        edit_reason.strip(),
                                })
                                flag_msg = f" ⚠️ Flagged ({correct_from} → {correct_to})" if flag_wrong else ""
                                st.success(f"✅ Updated.{flag_msg}")
                                st.session_state[ev_edit_key] = False
                                st.rerun()

                # Edit history for this evidence node
                ev_history = tc_load(eid)
                if ev_history:
                    for h in reversed(ev_history):
                        flag_note = " 🚩" if h.get("flagged_wrong") else ""
                        st.caption(f"_Edited {h.get('timestamp','')[:16]}{flag_note}: {h.get('reason','')[:100]}_")

                # ── Scalars ───────────────────────────────────────────────────
                scalars = run_query("""
                    MATCH (v:TransformationVector {vector_id: $vid})-[r:IMPACTS]->(sc:Scalar)
                    RETURN sc.scalar_id       AS scalar_id,
                           sc.name            AS name,
                           r.direction        AS direction,
                           r.impact_strength  AS strength,
                           r.impact_score     AS score,
                           r.rationale        AS rationale
                    ORDER BY r.impact_score DESC
                """, vid=vid)

                if scalars:
                    st.markdown("#### ⚙️ Causal Forces (Scalars)")
                    st.caption("The conditions pushing / pulling this transition")
                    for sc in scalars:
                        score    = sc.get("score") or 0
                        arrow    = "⬆️" if sc.get("direction") == "increases" else "⬇️"
                        strength = sc.get("strength") or "—"
                        col_sc1, col_sc2 = st.columns([3, 1])
                        with col_sc1:
                            st.markdown(f"{arrow} **{sc['name'][:70]}**")
                            rationale = sc.get("rationale") or ""
                            if rationale:
                                st.caption(rationale[:300])
                        with col_sc2:
                            color = "green" if score and score > 0 else "red"
                            st.markdown(f":{color}[**{strength}** ({'+' if score and score > 0 else ''}{score})]")

                    # Scalar edit toggle — keyed on vid+eid so each card is independent
                    sc_edit_key = f"show_sc_edit_{vid}_{eid}"
                    if sc_edit_key not in st.session_state:
                        st.session_state[sc_edit_key] = False
                    # Use vid+eid to give each card its own button key
                    if st.button("✏️ Edit scalars", key=f"btn_sc_edit_{vid}_{eid}"):
                        st.session_state[sc_edit_key] = not st.session_state[sc_edit_key]

                    if st.session_state[sc_edit_key]:
                        with st.form(key=f"sc_edit_form_{vid}_{eid}"):
                            st.markdown("**Edit scalar directions and rationale:**")
                            sc_edits = []
                            for sc in scalars:
                                st.markdown(f"**{sc['name'][:80]}**")
                                ec1, ec2 = st.columns(2)
                                cur_dir = sc.get("direction") or "increases"
                                cur_str = sc.get("strength") or "moderate"
                                new_dir = ec1.selectbox(
                                    "Direction", DIRECTION_OPTIONS,
                                    index=DIRECTION_OPTIONS.index(cur_dir) if cur_dir in DIRECTION_OPTIONS else 0,
                                    key=f"sc_dir_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                new_str = ec2.selectbox(
                                    "Strength", STRENGTH_OPTIONS,
                                    index=STRENGTH_OPTIONS.index(cur_str) if cur_str in STRENGTH_OPTIONS else 1,
                                    key=f"sc_str_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                new_rat = st.text_area(
                                    "Rationale", value=sc.get("rationale") or "",
                                    height=70, key=f"sc_rat_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                sc_edits.append({
                                    "scalar_id": sc["scalar_id"],
                                    "new_dir": new_dir,
                                    "new_str": new_str,
                                    "new_rat": new_rat,
                                })
                                st.divider()
                            sc_reason = st.text_area(
                                "🧠 Why are you correcting these scalars?", height=70,
                                placeholder="e.g. 'The marginal cost direction was inverted.'",
                            )
                            sc_submitted = st.form_submit_button("💾 Save scalar edits", type="primary")
                            if sc_submitted:
                                if not sc_reason.strip():
                                    st.error("Please explain your reason.")
                                else:
                                    now = datetime.now(timezone.utc).isoformat()
                                    for ed in sc_edits:
                                        new_score = IMPACT_SCORE_MAP.get((ed["new_dir"], ed["new_str"]), 0)
                                        run_query("""
                                            MATCH (v:TransformationVector {vector_id: $vid})
                                                  -[r:IMPACTS]->(sc:Scalar {scalar_id: $sid})
                                            SET r.direction       = $dir,
                                                r.impact_strength = $strength,
                                                r.impact_score    = $score,
                                                r.rationale       = $rationale,
                                                r.edited_at       = $now,
                                                r.edited_by       = 'editorial'
                                        """, vid=vid, sid=ed["scalar_id"],
                                            dir=ed["new_dir"], strength=ed["new_str"],
                                            score=new_score, rationale=ed["new_rat"], now=now)
                                    tc_append({
                                        "evidence_id":  eid,
                                        "vector_id":    vid,
                                        "from_bm":      case["from_bm"],
                                        "to_bm":        case["to_bm"],
                                        "timestamp":    now,
                                        "change_type":  "scalar_edit",
                                        "scalar_edits": [{"scalar_id": ed["scalar_id"],
                                                          "direction": ed["new_dir"],
                                                          "strength":  ed["new_str"]} for ed in sc_edits],
                                        "reason": sc_reason.strip(),
                                    })
                                    st.success(f"✅ {len(sc_edits)} scalar(s) updated.")
                                    st.session_state[sc_edit_key] = False
                                    st.rerun()

                # ── Hypothesis ────────────────────────────────────────────────
                if has_hyp:
                    st.markdown("#### 💡 Disruption Hypothesis")
                    hcol1, hcol2, hcol3 = st.columns(3)
                    hcol1.metric("Conviction", f"{case.get('conviction') or 0:.2f}")
                    hcol2.metric("Type", case.get("dtype") or "—")
                    hcol3.metric("Horizon", case.get("horizon") or "—")
                    if case.get("hyp_title"):
                        st.markdown(f"**{case['hyp_title']}**")
                    thesis = case.get("thesis") or ""
                    if thesis:
                        st.markdown(thesis)
                    counter = case.get("counter") or ""
                    if counter:
                        st.markdown("**⚠️ Counter-argument:**")
                        st.markdown(f"_{counter}_")
                    ai_link = case.get("ai_link") or ""
                    if ai_link:
                        st.markdown(f"**🤖 AI link:** {ai_link}")

    # ── Global transition changelog ───────────────────────────────────────────
    st.divider()
    st.subheader("📋 Transition Edit Log")
    if os.path.exists(tc_log_path):
        with open(tc_log_path) as f:
            all_tc = [json.loads(l) for l in f if l.strip()]
        if all_tc:
            st.caption(f"{len(all_tc)} total edits · {sum(1 for e in all_tc if e.get('flagged_wrong'))} flagged as wrong")
            st.dataframe(
                [{"Time":     e.get("timestamp","")[:19],
                  "From→To":  f"{e.get('from_bm','')} → {e.get('to_bm','')}",
                  "Companies": ", ".join(e.get("new_companies") or [])[:40],
                  "Flagged":  "🚩" if e.get("flagged_wrong") else "",
                  "Reason":   e.get("reason","")[:80]}
                 for e in reversed(all_tc)],
                use_container_width=True, hide_index=True
            )
    else:
        st.caption("No edits logged yet.")


# ── Page: Transformations ─────────────────────────────────────────────────────

elif page == "📐 Transformations":
    st.title("📐 Transformations")
    st.caption("Every recorded business model transition — evidence, causal forces, and hypothesis in one place.")

    # ── Changelog helpers ──────────────────────────────────────────────────────
    tv_log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "transformation_changelog.jsonl"
    )

    def tv_append(entry: dict):
        os.makedirs(os.path.dirname(tv_log_path), exist_ok=True)
        with open(tv_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def tv_load(vid: str) -> list:
        if not os.path.exists(tv_log_path):
            return []
        out = []
        with open(tv_log_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("vector_id") == vid:
                        out.append(e)
                except Exception:
                    pass
        return out

    # ── Filters ────────────────────────────────────────────────────────────────
    fcol1, fcol2, fcol3, fcol4 = st.columns([2, 2, 2, 2])
    with fcol1:
        tv_search = st.text_input("🔍 Search", placeholder="e.g. Netflix, SaaS, subscription…", key="tv_search")
    with fcol2:
        tv_has_evidence = st.checkbox("Evidence only", value=True, key="tv_has_evidence",
                                      help="Only show transformations that have at least one evidence node")
    with fcol3:
        tv_sort = st.selectbox("Sort by", ["Signal strength", "Evidence count"], key="tv_sort")
    with fcol4:
        tv_from_bms = run_query("MATCH (f:BusinessModel) RETURN f.name AS name ORDER BY f.name")
        tv_from_opts = ["All"] + [r["name"] for r in tv_from_bms]
        tv_from = st.selectbox("From BM", tv_from_opts, key="tv_from")

    # ── Query ──────────────────────────────────────────────────────────────────
    tv_from_filter   = "" if tv_from == "All" else "AND f.name = $from_bm"
    tv_evidence_filter = "AND evidence_count > 0" if tv_has_evidence else ""

    tv_sort_clause = {
        "Signal strength": "ORDER BY v.signal_strength DESC",
        "Evidence count":  "ORDER BY evidence_count DESC",
    }[tv_sort]

    transformations = run_query(f"""
        MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
        MATCH (v)-[:TO_BIM]->(t:BusinessModel)
        WHERE 1=1 {tv_from_filter}
        WITH v, f, t,
             size([(e:Evidence)-[:SUPPORTS]->(v)|e])   AS evidence_count,
             size([(sc:Scalar)<-[:IMPACTS]-(v)|sc])    AS scalar_count
        WHERE 1=1 {tv_evidence_filter}
        OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
        WITH v, f, t, evidence_count, scalar_count, head(collect(h)) AS h
        RETURN v.vector_id          AS vid,
               v.signal_strength    AS signal,
               v.description        AS description,
               v.example_text       AS example_text,
               f.name               AS from_bm,
               f.bim_id             AS from_id,
               t.name               AS to_bm,
               t.bim_id             AS to_id,
               evidence_count,
               scalar_count,
               h.hypothesis_id      AS hyp_id,
               h.title              AS hyp_title,
               h.conviction_score   AS conviction,
               h.thesis             AS thesis,
               h.counter_argument   AS counter,
               h.disruption_type    AS dtype,
               h.time_horizon       AS horizon,
               h.ai_disruption_link AS ai_link
        {tv_sort_clause}
    """, from_bm=tv_from)

    if tv_search:
        q = tv_search.lower()
        transformations = [t for t in transformations if
                           q in (t.get("from_bm") or "").lower()
                           or q in (t.get("to_bm") or "").lower()
                           or q in (t.get("example_text") or "").lower()
                           or q in (t.get("description") or "").lower()
                           or q in (t.get("hyp_title") or "").lower()]

    st.divider()
    st.caption(f"{len(transformations)} transformations  ·  "
               f"{sum(t.get('evidence_count') or 0 for t in transformations)} evidence nodes")

    TV_DIRECTION_OPTIONS = ["increases", "neutral", "decreases"]
    TV_STRENGTH_OPTIONS  = ["strong", "moderate", "weak"]
    TV_IMPACT_SCORE_MAP  = {
        ("increases", "strong"): 2, ("increases", "moderate"): 1,
        ("neutral", "strong"): 0, ("neutral", "moderate"): 0, ("neutral", "weak"): 0,
        ("decreases", "moderate"): -1, ("decreases", "strong"): -2,
    }

    for tr in transformations:
        vid      = tr.get("vid") or ""
        signal   = tr.get("signal") or 0
        ev_count = tr.get("evidence_count") or 0
        sc_count = tr.get("scalar_count") or 0
        has_hyp  = bool(tr.get("hyp_id"))

        label = f"{tr['from_bm']} → {tr['to_bm']}  ·  signal {signal:.3f}  ·  {ev_count} evidence"
        with st.expander(label, expanded=False):
            st.markdown(f"<u>**{tr['from_bm']} → {tr['to_bm']}**</u>", unsafe_allow_html=True)

            m1, m2, m3 = st.columns(3)
            m1.metric("Signal", f"{signal:.3f}")
            m2.metric("Evidence", ev_count)
            m3.metric("Scalars", sc_count)

            # General description (editable, separate from example)
            desc = tr.get("description") or ""
            if desc:
                st.markdown("#### 📝 Description")
                st.markdown(desc)

            ex = tr.get("example_text") or ""
            if ex:
                st.markdown("#### 📌 Example")
                st.caption(ex)
            if desc or ex:
                st.divider()

            # Evidence
            if ev_count > 0:
                st.markdown("#### 🏢 Evidence")
                ev_rows = run_query("""
                    MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector {vector_id: $vid})
                    RETURN e.companies_mentioned AS companies,
                           e.transition_summary  AS summary,
                           e.evidence_quote      AS quote,
                           e.source_url          AS url,
                           e.confidence          AS conf
                    ORDER BY e.confidence DESC
                """, vid=vid)
                for ev in ev_rows:
                    cos = ev.get("companies") or []
                    if isinstance(cos, str):
                        try: cos = json.loads(cos)
                        except Exception: cos = [cos]
                    co_str  = ", ".join(cos) if cos else "Unknown"
                    summary = ev.get("summary") or ""
                    ec1, ec2 = st.columns([5, 1])
                    with ec1:
                        st.markdown(f"**🏢 {co_str}** — {summary[:180] if summary else '_No summary_'}")
                        quote = ev.get("quote") or ""
                        if quote and quote.strip() != summary.strip():
                            st.markdown(f"> \"{quote[:250]}\"")
                        src = ev.get("url") or ""
                        if src:
                            st.caption(f"🔗 [{src[:70]}]({src})")
                    with ec2:
                        st.caption(f"conf: {ev.get('conf') or 0:.2f}")
                st.divider()

            # Scalars
            scalars = run_query("""
                MATCH (v:TransformationVector {vector_id: $vid})-[r:IMPACTS]->(sc:Scalar)
                RETURN sc.scalar_id AS scalar_id, sc.name AS name,
                       r.direction AS direction, r.impact_strength AS strength,
                       r.impact_score AS score, r.rationale AS rationale
                ORDER BY r.impact_score DESC
            """, vid=vid)

            if scalars:
                st.markdown("#### ⚙️ Causal Forces")
                for sc in scalars:
                    score = sc.get("score") or 0
                    arrow = "⬆️" if sc.get("direction") == "increases" else "⬇️"
                    sc1, sc2 = st.columns([3, 1])
                    with sc1:
                        st.markdown(f"{arrow} **{sc['name'][:70]}**")
                        if sc.get("rationale"):
                            st.caption(sc["rationale"][:280])
                    with sc2:
                        color = "green" if score > 0 else ("red" if score < 0 else "gray")
                        st.markdown(f":{color}[**{sc.get('strength','—')}** ({'+' if score > 0 else ''}{score})]")

                tv_sc_key = f"tv_sc_edit_{vid}"
                if tv_sc_key not in st.session_state:
                    st.session_state[tv_sc_key] = False
                if st.button("✏️ Edit scalars", key=f"tv_btn_sc_{vid}"):
                    st.session_state[tv_sc_key] = not st.session_state[tv_sc_key]
                if st.session_state[tv_sc_key]:
                    with st.form(key=f"tv_sc_form_{vid}"):
                        sc_edits = []
                        for sc in scalars:
                            st.markdown(f"**{sc['name'][:80]}**")
                            dc1, dc2 = st.columns(2)
                            cur_dir = sc.get("direction") or "increases"
                            cur_str = sc.get("strength") or "moderate"
                            nd = dc1.selectbox("Direction", TV_DIRECTION_OPTIONS,
                                index=TV_DIRECTION_OPTIONS.index(cur_dir) if cur_dir in TV_DIRECTION_OPTIONS else 0,
                                key=f"tv_sc_dir_{vid}_{sc['scalar_id']}")
                            ns = dc2.selectbox("Strength", TV_STRENGTH_OPTIONS,
                                index=TV_STRENGTH_OPTIONS.index(cur_str) if cur_str in TV_STRENGTH_OPTIONS else 1,
                                key=f"tv_sc_str_{vid}_{sc['scalar_id']}")
                            nr = st.text_area("Rationale", value=sc.get("rationale") or "",
                                height=60, key=f"tv_sc_rat_{vid}_{sc['scalar_id']}")
                            sc_edits.append({"scalar_id": sc["scalar_id"],
                                             "new_dir": nd, "new_str": ns, "new_rat": nr})
                            st.divider()
                        sc_reason = st.text_area("🧠 Reason", height=60,
                            placeholder="Why are you correcting these scalars?")
                        if st.form_submit_button("💾 Save scalar edits", type="primary"):
                            if not sc_reason.strip():
                                st.error("Please enter a reason.")
                            else:
                                now = datetime.now(timezone.utc).isoformat()
                                for ed in sc_edits:
                                    new_score = TV_IMPACT_SCORE_MAP.get((ed["new_dir"], ed["new_str"]), 0)
                                    run_query("""
                                        MATCH (v:TransformationVector {vector_id: $vid})
                                              -[r:IMPACTS]->(sc:Scalar {scalar_id: $sid})
                                        SET r.direction=$dir, r.impact_strength=$strength,
                                            r.impact_score=$score, r.rationale=$rationale,
                                            r.edited_at=$now, r.edited_by='editorial'
                                    """, vid=vid, sid=ed["scalar_id"], dir=ed["new_dir"],
                                        strength=ed["new_str"], score=new_score,
                                        rationale=ed["new_rat"], now=now)
                                tv_append({"vector_id": vid, "from_bm": tr["from_bm"],
                                           "to_bm": tr["to_bm"], "timestamp": now,
                                           "change_type": "scalar_edit",
                                           "scalar_edits": [{"scalar_id": e["scalar_id"],
                                               "direction": e["new_dir"], "strength": e["new_str"]}
                                               for e in sc_edits],
                                           "reason": sc_reason.strip()})
                                st.success(f"✅ {len(sc_edits)} scalar(s) updated.")
                                st.session_state[tv_sc_key] = False
                                st.rerun()
                st.divider()

            # Description edit (always available, not tied to hypothesis)
            tv_desc_key = f"tv_desc_edit_{vid}"
            if tv_desc_key not in st.session_state:
                st.session_state[tv_desc_key] = False
            if st.button("✏️ Edit description", key=f"tv_btn_desc_{vid}"):
                st.session_state[tv_desc_key] = not st.session_state[tv_desc_key]
            if st.session_state[tv_desc_key]:
                with st.form(key=f"tv_desc_form_{vid}"):
                    new_desc = st.text_area(
                        "General description of this transition pattern",
                        value=desc,
                        height=120,
                        help="Describe what drives this transition in general terms — not tied to one company",
                        placeholder="e.g. 'Companies with mature software products shift from one-time licence fees to recurring subscription revenue. The key driver is predictable cashflow and lower barrier to adoption…'",
                    )
                    desc_reason = st.text_area("🧠 Reason / source", height=60,
                        placeholder="e.g. 'Written after reviewing 4 evidence nodes.'")
                    if st.form_submit_button("💾 Save description", type="primary"):
                        if not desc_reason.strip():
                            st.error("Please enter a reason.")
                        else:
                            now = datetime.now(timezone.utc).isoformat()
                            run_query("""
                                MATCH (v:TransformationVector {vector_id: $vid})
                                SET v.description=$desc, v.last_edited_at=$now,
                                    v.last_edited_by='editorial'
                            """, vid=vid, desc=new_desc, now=now)
                            tv_append({"vector_id": vid, "from_bm": tr["from_bm"],
                                       "to_bm": tr["to_bm"], "timestamp": now,
                                       "change_type": "description_edit",
                                       "old_desc": desc, "new_desc": new_desc,
                                       "reason": desc_reason.strip()})
                            st.success("✅ Description saved.")
                            st.session_state[tv_desc_key] = False
                            st.rerun()

            # Hypothesis
            if has_hyp:
                st.markdown("#### 💡 Disruption Hypothesis")
                hc1, hc2, hc3 = st.columns(3)
                hc1.metric("Conviction", f"{tr.get('conviction') or 0:.2f}")
                hc2.metric("Type", tr.get("dtype") or "—")
                hc3.metric("Horizon", tr.get("horizon") or "—")
                if tr.get("hyp_title"):
                    st.markdown(f"**{tr['hyp_title']}**")
                if tr.get("thesis"):
                    st.markdown(tr["thesis"])
                if tr.get("counter"):
                    st.markdown(f"**⚠️ Counter:** _{tr['counter']}_")
                if tr.get("ai_link"):
                    st.markdown(f"**🤖 AI link:** {tr['ai_link']}")

                tv_hyp_key = f"tv_hyp_edit_{vid}"
                if tv_hyp_key not in st.session_state:
                    st.session_state[tv_hyp_key] = False
                if st.button("✏️ Edit hypothesis", key=f"tv_btn_hyp_{vid}"):
                    st.session_state[tv_hyp_key] = not st.session_state[tv_hyp_key]
                if st.session_state[tv_hyp_key]:
                    with st.form(key=f"tv_hyp_form_{vid}"):
                        new_title   = st.text_input("Title", value=tr.get("hyp_title") or "")
                        new_thesis  = st.text_area("Thesis", value=tr.get("thesis") or "", height=100)
                        new_counter = st.text_area("Counter-argument", value=tr.get("counter") or "", height=80)
                        new_ai_link = st.text_input("AI disruption link", value=tr.get("ai_link") or "")
                        hfc1, hfc2 = st.columns(2)
                        dtype_opts   = ["Substitution", "Compression", "Disintermediation",
                                        "Platform shift", "Bundling", "Unbundling", "Other"]
                        horizon_opts = ["< 2 years", "2–5 years", "5–10 years", "> 10 years"]
                        cur_dtype   = tr.get("dtype") or "Other"
                        cur_horizon = tr.get("horizon") or "2–5 years"
                        new_dtype   = hfc1.selectbox("Disruption type", dtype_opts,
                            index=dtype_opts.index(cur_dtype) if cur_dtype in dtype_opts else len(dtype_opts)-1,
                            key=f"tv_dtype_{vid}")
                        new_horizon = hfc2.selectbox("Time horizon", horizon_opts,
                            index=horizon_opts.index(cur_horizon) if cur_horizon in horizon_opts else 1,
                            key=f"tv_horizon_{vid}")
                        hyp_reason = st.text_area("🧠 Reason for change", height=60,
                            placeholder="e.g. 'Updated thesis to reflect 2025 AI cost curves.'")
                        if st.form_submit_button("💾 Save hypothesis edits", type="primary"):
                            if not hyp_reason.strip():
                                st.error("Please enter a reason.")
                            else:
                                now = datetime.now(timezone.utc).isoformat()
                                run_query("""
                                    MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                                    SET h.title=$title, h.thesis=$thesis,
                                        h.counter_argument=$counter,
                                        h.ai_disruption_link=$ai_link,
                                        h.disruption_type=$dtype,
                                        h.time_horizon=$horizon,
                                        h.last_edited_at=$now,
                                        h.last_edited_by='editorial'
                                """, hid=tr["hyp_id"], title=new_title,
                                    thesis=new_thesis, counter=new_counter,
                                    ai_link=new_ai_link, dtype=new_dtype,
                                    horizon=new_horizon, now=now)
                                tv_append({"vector_id": vid, "from_bm": tr["from_bm"],
                                           "to_bm": tr["to_bm"], "timestamp": now,
                                           "change_type": "hypothesis_edit",
                                           "hypothesis_id": tr["hyp_id"],
                                           "old_title": tr.get("hyp_title"),
                                           "new_title": new_title,
                                           "reason": hyp_reason.strip()})
                                st.success("✅ Hypothesis updated.")
                                st.session_state[tv_hyp_key] = False
                                st.rerun()

            # Edit history
            tv_history = tv_load(vid)
            if tv_history:
                st.divider()
                st.markdown(f"**📋 Edit history ({len(tv_history)})**")
                for h in reversed(tv_history[-5:]):
                    st.caption(f"_{h.get('timestamp','')[:16]}  [{h.get('change_type','')}]  {h.get('reason','')[:100]}_")

    st.divider()
    st.subheader("📋 Transformation Edit Log")
    if os.path.exists(tv_log_path):
        with open(tv_log_path) as f:
            all_tv = [json.loads(l) for l in f if l.strip()]
        if all_tv:
            st.dataframe([{
                "Time":        e.get("timestamp","")[:16],
                "Vector":      f"{e.get('from_bm','')} → {e.get('to_bm','')}",
                "Change type": e.get("change_type",""),
                "Reason":      e.get("reason","")[:80],
            } for e in reversed(all_tv)], use_container_width=True, hide_index=True)
    else:
        st.caption("No edits logged yet.")


# ── Page: Scalars ──────────────────────────────────────────────────────────────

elif page == "⚡ Scalars":
    st.title("⚡ Scalars")
    st.caption("The structural conditions that drive business model transitions.")

    sc_log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "scalar_changelog.jsonl"
    )

    def sc_append(entry: dict):
        os.makedirs(os.path.dirname(sc_log_path), exist_ok=True)
        with open(sc_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def sc_load_history(scalar_id: str) -> list:
        if not os.path.exists(sc_log_path):
            return []
        out = []
        with open(sc_log_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("scalar_id") == scalar_id:
                        out.append(e)
                except Exception:
                    pass
        return out

    # Filters
    sfc1, sfc2, sfc3 = st.columns([3, 2, 2])
    with sfc1:
        sc_search = st.text_input("🔍 Search scalars", placeholder="e.g. marginal cost, network…", key="sc_search")
    with sfc2:
        sc_groups = run_query("MATCH (sc:Scalar) RETURN DISTINCT sc.group AS g ORDER BY g")
        group_opts = ["All groups"] + [r["g"] for r in sc_groups if r.get("g")]
        sc_group = st.selectbox("Group", group_opts, key="sc_group")
    with sfc3:
        sc_sort = st.selectbox("Sort by", ["Name", "Trend strength", "# transitions"], key="sc_sort")

    sc_group_filter = "" if sc_group == "All groups" else "AND sc.group = $sc_group"
    sc_sort_clause  = {
        "Name":           "ORDER BY sc.name",
        "Trend strength": "ORDER BY sc.trend_strength DESC",
        "# transitions":  "ORDER BY vector_count DESC",
    }[sc_sort]

    all_scalars = run_query(f"""
        MATCH (sc:Scalar)
        WHERE 1=1 {sc_group_filter}
        WITH sc,
             size([(v:TransformationVector)-[:IMPACTS]->(sc)|v]) AS vector_count
        RETURN sc.scalar_id       AS scalar_id,
               sc.name            AS name,
               sc.description     AS description,
               sc.rationale       AS rationale,
               sc.group           AS group,
               sc.code            AS code,
               sc.trend_direction AS trend_direction,
               sc.trend_strength  AS trend_strength,
               vector_count
        {sc_sort_clause}
    """, sc_group=sc_group)

    if sc_search:
        q = sc_search.lower()
        all_scalars = [s for s in all_scalars if
                       q in (s.get("name") or "").lower()
                       or q in (s.get("description") or "").lower()
                       or q in (s.get("rationale") or "").lower()]

    st.divider()
    st.caption(f"{len(all_scalars)} scalars")

    for scalar in all_scalars:
        sid         = scalar.get("scalar_id") or ""
        sc_name     = (scalar.get("name") or sid).replace("\n", " ")
        sc_desc     = scalar.get("description") or ""
        sc_rat      = scalar.get("rationale") or ""
        vec_count   = scalar.get("vector_count") or 0
        trend_dir   = scalar.get("trend_direction") or "—"
        trend_str   = scalar.get("trend_strength") or 0
        trend_arrow = "⬆️" if trend_dir == "increases" else ("⬇️" if trend_dir == "decreases" else "➡️")

        card_label = f"{scalar.get('code','?')} · {sc_name[:80]}  ·  {vec_count} transitions"
        with st.expander(card_label, expanded=False):
            st.markdown(f"<u>**{sc_name}**</u>", unsafe_allow_html=True)

            sm1, sm2, sm3 = st.columns(3)
            sm1.metric("Transitions", vec_count)
            sm2.metric("Trend", f"{trend_arrow} {trend_dir}")
            sm3.metric("Trend strength", f"{trend_str:.2f}" if trend_str else "—")

            if sc_desc:
                st.markdown("**Description**")
                st.markdown(sc_desc.replace("\n", "  \n"))
            if sc_rat:
                st.markdown("**Why it matters**")
                st.caption(sc_rat)

            st.divider()

            # Linked transformations
            linked = run_query("""
                MATCH (v:TransformationVector)-[r:IMPACTS]->(sc:Scalar {scalar_id: $sid})
                MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
                MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                RETURN f.name AS from_bm, t.name AS to_bm,
                       v.signal_strength AS signal,
                       r.direction AS direction, r.impact_strength AS strength,
                       r.impact_score AS score, r.rationale AS rationale
                ORDER BY r.impact_score DESC
            """, sid=sid)

            if linked:
                st.markdown(f"#### 🔗 Transformations this scalar influences ({len(linked)})")

                def render_linked(rows, header):
                    if not rows:
                        return
                    st.markdown(f"**{header}**")
                    for lv in rows:
                        score = lv.get("score") or 0
                        arrow = "⬆️" if lv.get("direction") == "increases" else "⬇️"
                        color = "green" if score > 0 else ("red" if score < 0 else "gray")
                        lc1, lc2 = st.columns([4, 1])
                        with lc1:
                            st.markdown(f"{arrow} **{lv['from_bm']} → {lv['to_bm']}**")
                            if lv.get("rationale"):
                                st.caption(lv["rationale"][:250])
                        with lc2:
                            st.markdown(f":{color}[**{lv.get('strength','—')}** ({'+' if score > 0 else ''}{score})]")
                            st.caption(f"sig: {lv.get('signal') or 0:.3f}")

                pos = [l for l in linked if (l.get("score") or 0) > 0]
                neg = [l for l in linked if (l.get("score") or 0) < 0]
                neu = [l for l in linked if (l.get("score") or 0) == 0]
                render_linked(pos, "✅ Positive — increases probability of this transition")
                render_linked(neg, "❌ Negative — decreases probability of this transition")
                render_linked(neu, "➡️ Neutral")

            st.divider()

            # Edit scalar definition
            sc_def_key = f"sc_def_edit_{sid}"
            if sc_def_key not in st.session_state:
                st.session_state[sc_def_key] = False
            if st.button("✏️ Edit scalar definition", key=f"btn_sc_def_{sid}"):
                st.session_state[sc_def_key] = not st.session_state[sc_def_key]

            if st.session_state[sc_def_key]:
                with st.form(key=f"sc_def_form_{sid}"):
                    new_name = st.text_area("Name", value=sc_name, height=70)
                    new_desc = st.text_area("Description", value=sc_desc, height=100)
                    new_rat  = st.text_area("Why it matters (rationale)", value=sc_rat, height=80)
                    def_reason = st.text_area("🧠 Reason for change", height=60,
                        placeholder="e.g. 'Clarified that marginal cost refers to digital distribution only.'")
                    if st.form_submit_button("💾 Save", type="primary"):
                        if not def_reason.strip():
                            st.error("Please enter a reason.")
                        else:
                            now = datetime.now(timezone.utc).isoformat()
                            run_query("""
                                MATCH (sc:Scalar {scalar_id: $sid})
                                SET sc.name=$name, sc.description=$desc,
                                    sc.rationale=$rat, sc.last_edited_at=$now,
                                    sc.last_edited_by='editorial'
                            """, sid=sid, name=new_name, desc=new_desc, rat=new_rat, now=now)
                            sc_append({"scalar_id": sid, "timestamp": now,
                                       "change_type": "definition_edit",
                                       "old_name": sc_name, "new_name": new_name,
                                       "reason": def_reason.strip()})
                            st.success("✅ Scalar updated.")
                            st.session_state[sc_def_key] = False
                            st.rerun()

            sc_hist = sc_load_history(sid)
            if sc_hist:
                st.markdown(f"**📋 Edit history ({len(sc_hist)})**")
                for h in reversed(sc_hist[-5:]):
                    st.caption(f"_{h.get('timestamp','')[:16]}  {h.get('reason','')[:100]}_")

    st.divider()
    st.subheader("📋 Scalar Edit Log")
    if os.path.exists(sc_log_path):
        with open(sc_log_path) as f:
            all_sc_log = [json.loads(l) for l in f if l.strip()]
        if all_sc_log:
            st.dataframe([{
                "Time":    e.get("timestamp","")[:16],
                "Scalar":  e.get("scalar_id",""),
                "Change":  e.get("change_type",""),
                "Reason":  e.get("reason","")[:80],
            } for e in reversed(all_sc_log)], use_container_width=True, hide_index=True)
    else:
        st.caption("No edits logged yet.")


# ── Page: Technologies ────────────────────────────────────────────────────────

elif page == "🔬 Technologies":
    st.title("🔬 Technology Transformations")
    st.caption("Technologies that are enabling or forcing business model transitions — what they are, what they disrupt, and how mature they are.")

    import anthropic as _anthropic

    tech_log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "technology_changelog.jsonl"
    )

    def tech_append(entry: dict):
        os.makedirs(os.path.dirname(tech_log_path), exist_ok=True)
        with open(tech_log_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")

    def tech_load_history(tech_id: str) -> list:
        if not os.path.exists(tech_log_path):
            return []
        out = []
        with open(tech_log_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("tech_id") == tech_id:
                        out.append(e)
                except Exception:
                    pass
        return out

    def next_tech_id() -> str:
        rows = run_query("MATCH (t:Technology) RETURN t.tech_id AS id ORDER BY id DESC LIMIT 1")
        if not rows or not rows[0].get("id"):
            return "TECH_001"
        last = rows[0]["id"]
        try:
            n = int(last.split("_")[1]) + 1
            return f"TECH_{n:03d}"
        except Exception:
            return f"TECH_{len(rows)+1:03d}"

    # ── Filters ────────────────────────────────────────────────────────────────
    tc1, tc2, tc3 = st.columns([3, 2, 2])
    with tc1:
        tech_search = st.text_input("🔍 Search technologies", placeholder="e.g. RAG, graph, LLM…", key="tech_search")
    with tc2:
        cat_rows = run_query("MATCH (t:Technology) RETURN DISTINCT t.category AS c ORDER BY c")
        cat_opts = ["All categories"] + [r["c"] for r in cat_rows if r.get("c")]
        tech_cat = st.selectbox("Category", cat_opts, key="tech_cat")
    with tc3:
        tech_sort = st.selectbox("Sort by", ["Maturity (high→low)", "Name", "Recently added"], key="tech_sort")

    cat_filter  = "" if tech_cat == "All categories" else "AND t.category = $cat"
    sort_clause = {
        "Maturity (high→low)": "ORDER BY t.maturity_level DESC",
        "Name":                "ORDER BY t.name",
        "Recently added":      "ORDER BY t.created_at DESC",
    }[tech_sort]

    techs = run_query(f"""
        MATCH (t:Technology)
        WHERE 1=1 {cat_filter}
        RETURN t.tech_id        AS tech_id,
               t.name           AS name,
               t.short_name     AS short_name,
               t.category       AS category,
               t.description    AS description,
               t.disruption_thesis AS disruption_thesis,
               t.primary_use_cases AS use_cases,
               t.key_players    AS key_players,
               t.maturity_level AS maturity,
               t.maturity_rationale AS maturity_rationale,
               t.confidence     AS confidence,
               t.tracking_status AS status,
               t.created_at     AS created_at
        {sort_clause}
    """, cat=tech_cat)

    if tech_search:
        q = tech_search.lower()
        techs = [t for t in techs if
                 q in (t.get("name") or "").lower()
                 or q in (t.get("description") or "").lower()
                 or q in (t.get("disruption_thesis") or "").lower()
                 or q in (t.get("short_name") or "").lower()]

    st.divider()
    st.caption(f"{len(techs)} technologies tracked")

    # ── Technology cards ───────────────────────────────────────────────────────
    for tech in techs:
        tid       = tech.get("tech_id") or ""
        t_name    = tech.get("name") or tid
        t_short   = tech.get("short_name") or ""
        maturity  = tech.get("maturity") or 0
        t_status  = tech.get("status") or "Active"
        t_cat     = tech.get("category") or "—"
        t_desc    = tech.get("description") or ""
        t_thesis  = tech.get("disruption_thesis") or ""
        use_cases = tech.get("use_cases") or []
        if isinstance(use_cases, str):
            try:    use_cases = json.loads(use_cases)
            except Exception: use_cases = [use_cases]
        key_players = tech.get("key_players") or []
        if isinstance(key_players, str):
            try:    key_players = json.loads(key_players)
            except Exception: key_players = [key_players]

        mat_bar  = "█" * int(maturity / 10) + "░" * (10 - int(maturity / 10))
        mat_color = "green" if maturity >= 70 else ("orange" if maturity >= 40 else "red")
        card_label = f"{'[' + t_short + '] ' if t_short else ''}{t_name}  ·  {t_cat}  ·  maturity {int(maturity)}%"

        with st.expander(card_label, expanded=False):
            st.markdown(f"<u>**{t_name}**</u>{'  `' + t_short + '`' if t_short else ''}", unsafe_allow_html=True)

            # Stats row
            tm1, tm2, tm3 = st.columns(3)
            tm1.markdown(f"**Maturity** :{mat_color}[{int(maturity)}% {mat_bar}]")
            tm2.metric("Category", t_cat)
            tm3.metric("Status", t_status)

            st.divider()

            if t_desc:
                st.markdown("**What it is**")
                st.markdown(t_desc)

            if t_thesis:
                st.markdown("**Disruption thesis**")
                st.info(t_thesis)

            if use_cases:
                st.markdown("**Primary use cases**")
                for uc in use_cases:
                    st.markdown(f"- {uc}")

            if key_players:
                st.markdown("**Key players**")
                st.markdown("  ·  ".join(key_players))

            if tech.get("maturity_rationale"):
                st.caption(f"_Maturity source: {tech['maturity_rationale'][:200]}_")

            st.divider()

            # ── Linked transformations ─────────────────────────────────────────
            # Vectors with INFLUENCES rel
            linked_vecs = run_query("""
                MATCH (t:Technology {tech_id: $tid})-[r:INFLUENCES]->(v:TransformationVector)
                MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
                MATCH (v)-[:TO_BIM]->(to:BusinessModel)
                RETURN f.name AS from_bm, to.name AS to_bm,
                       v.vector_id AS vid, v.signal_strength AS signal,
                       r.influence_type AS itype, r.rationale AS rationale,
                       r.confidence AS conf
                ORDER BY signal DESC
            """, tid=tid)

            # Also surface hypotheses whose ai_disruption_link or thesis mentions this tech
            name_fragments = [w for w in (t_name + " " + t_short).split() if len(w) > 3]
            hyp_matches = []
            if name_fragments:
                search_pat = "|".join(name_fragments[:4])
                hyp_matches = run_query(f"""
                    MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v:TransformationVector)
                    MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
                    MATCH (v)-[:TO_BIM]->(to:BusinessModel)
                    WHERE h.ai_disruption_link IS NOT NULL
                      AND toLower(h.ai_disruption_link + ' ' + coalesce(h.thesis,'')) =~ $pat
                    RETURN f.name AS from_bm, to.name AS to_bm,
                           v.vector_id AS vid, h.title AS hyp_title,
                           h.conviction_score AS conviction,
                           h.ai_disruption_link AS ai_link
                    ORDER BY conviction DESC
                    LIMIT 10
                """, pat=f".*({'|'.join(name_fragments[:3]).lower()}).*")

            if linked_vecs or hyp_matches:
                st.markdown(f"#### 🔗 Transformations this technology influences")
                if linked_vecs:
                    for lv in linked_vecs:
                        lc1, lc2 = st.columns([4, 1])
                        with lc1:
                            itype = lv.get("itype") or "enables"
                            st.markdown(f"**{lv['from_bm']} → {lv['to_bm']}**  _{itype}_")
                            if lv.get("rationale"):
                                st.caption(lv["rationale"][:200])
                        with lc2:
                            st.caption(f"sig: {lv.get('signal') or 0:.3f}")
                if hyp_matches:
                    st.markdown("_From hypothesis AI-link mentions:_")
                    for hm in hyp_matches:
                        st.markdown(f"- **{hm['from_bm']} → {hm['to_bm']}** — {hm.get('hyp_title','')[:80]}")
                        if hm.get("ai_link"):
                            st.caption(hm["ai_link"][:180])
            else:
                st.caption("_No linked transformations yet — link one below._")

            # Link to a vector
            link_key = f"tech_link_{tid}"
            if link_key not in st.session_state:
                st.session_state[link_key] = False
            if st.button("➕ Link to a transformation", key=f"btn_tech_link_{tid}"):
                st.session_state[link_key] = not st.session_state[link_key]
            if st.session_state[link_key]:
                all_vecs = run_query("""
                    MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
                    MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                    RETURN v.vector_id AS vid, f.name + ' → ' + t.name AS label
                    ORDER BY label
                """)
                vec_options = {r["label"]: r["vid"] for r in all_vecs}
                with st.form(key=f"tech_link_form_{tid}"):
                    selected_vec_label = st.selectbox("Transformation", list(vec_options.keys()),
                                                      key=f"tech_link_sel_{tid}")
                    itype_opts = ["enables", "accelerates", "disrupts", "commoditises", "new entrant threat"]
                    link_itype = st.selectbox("Influence type", itype_opts, key=f"tech_link_itype_{tid}")
                    link_rat   = st.text_area("Rationale", height=80,
                        placeholder="e.g. 'RAG enables near-zero-cost knowledge retrieval, removing the value proposition of traditional KM subscriptions.'")
                    link_conf  = st.slider("Confidence", 0.0, 1.0, 0.7, 0.05, key=f"tech_link_conf_{tid}")
                    if st.form_submit_button("💾 Create link", type="primary"):
                        if not link_rat.strip():
                            st.error("Please enter a rationale.")
                        else:
                            now = datetime.now(timezone.utc).isoformat()
                            run_query("""
                                MATCH (t:Technology {tech_id: $tid})
                                MATCH (v:TransformationVector {vector_id: $vid})
                                MERGE (t)-[r:INFLUENCES]->(v)
                                SET r.influence_type=$itype, r.rationale=$rat,
                                    r.confidence=$conf, r.created_at=$now, r.created_by='editorial'
                            """, tid=tid, vid=vec_options[selected_vec_label],
                                itype=link_itype, rat=link_rat.strip(), conf=link_conf, now=now)
                            tech_append({"tech_id": tid, "timestamp": now,
                                         "change_type": "link_added",
                                         "vector_id": vec_options[selected_vec_label],
                                         "influence_type": link_itype,
                                         "reason": link_rat.strip()})
                            st.success("✅ Link created.")
                            st.session_state[link_key] = False
                            st.rerun()

            st.divider()

            # ── Edit technology ────────────────────────────────────────────────
            te_key = f"tech_edit_{tid}"
            if te_key not in st.session_state:
                st.session_state[te_key] = False
            if st.button("✏️ Edit technology", key=f"btn_tech_edit_{tid}"):
                st.session_state[te_key] = not st.session_state[te_key]
            if st.session_state[te_key]:
                with st.form(key=f"tech_edit_form_{tid}"):
                    en1, en2 = st.columns(2)
                    new_name  = en1.text_input("Name", value=t_name)
                    new_short = en2.text_input("Short name / acronym", value=t_short)
                    new_desc   = st.text_area("What it is", value=t_desc, height=120)
                    new_thesis = st.text_area("Disruption thesis", value=t_thesis, height=100)
                    new_uc = st.text_area("Primary use cases (one per line)",
                                          value="\n".join(use_cases), height=100)
                    new_kp = st.text_area("Key players (one per line)",
                                          value="\n".join(key_players), height=80)
                    em1, em2 = st.columns(2)
                    cat_edit_opts = sorted(set(cat_opts[1:] + ["AI/ML", "Infrastructure", "Data", "Hardware", "Other"]))
                    new_cat = em1.selectbox("Category", cat_edit_opts,
                        index=cat_edit_opts.index(t_cat) if t_cat in cat_edit_opts else 0,
                        key=f"tech_cat_edit_{tid}")
                    new_mat = em2.slider("Maturity level (0–100)", 0, 100, int(maturity or 0),
                                         key=f"tech_mat_edit_{tid}")
                    new_mat_rat = st.text_area("Maturity rationale / source",
                                               value=tech.get("maturity_rationale") or "", height=70)
                    edit_reason = st.text_area("🧠 Reason for change", height=60,
                        placeholder="e.g. 'Updated maturity to 80 after Gartner Hype Cycle 2025 placed RAG in Slope of Enlightenment.'")
                    if st.form_submit_button("💾 Save", type="primary"):
                        if not edit_reason.strip():
                            st.error("Please enter a reason.")
                        else:
                            now = datetime.now(timezone.utc).isoformat()
                            new_uc_list = [l.strip() for l in new_uc.split("\n") if l.strip()]
                            new_kp_list = [l.strip() for l in new_kp.split("\n") if l.strip()]
                            run_query("""
                                MATCH (t:Technology {tech_id: $tid})
                                SET t.name=$name, t.short_name=$short,
                                    t.description=$desc, t.disruption_thesis=$thesis,
                                    t.primary_use_cases=$uc, t.key_players=$kp,
                                    t.category=$cat, t.maturity_level=$mat,
                                    t.maturity_rationale=$mat_rat,
                                    t.updated_at=$now, t.last_edited_by='editorial'
                            """, tid=tid, name=new_name, short=new_short, desc=new_desc,
                                thesis=new_thesis, uc=new_uc_list, kp=new_kp_list,
                                cat=new_cat, mat=float(new_mat), mat_rat=new_mat_rat, now=now)
                            tech_append({"tech_id": tid, "name": t_name, "timestamp": now,
                                         "change_type": "edit", "reason": edit_reason.strip()})
                            st.success("✅ Technology updated.")
                            st.session_state[te_key] = False
                            st.rerun()

            th = tech_load_history(tid)
            if th:
                st.markdown(f"**📋 Edit history ({len(th)})**")
                for h in reversed(th[-4:]):
                    st.caption(f"_{h.get('timestamp','')[:16]}  [{h.get('change_type','')}]  {h.get('reason','')[:90]}_")

    # ── Add Technology ─────────────────────────────────────────────────────────
    st.divider()
    st.subheader("➕ Add Technology")
    add_tab1, add_tab2 = st.tabs(["📝 Manual form", "📄 Extract from text / file"])

    with add_tab1:
        with st.form("tech_add_form"):
            st.markdown("**Basic information**")
            af1, af2 = st.columns(2)
            add_name  = af1.text_input("Technology name *", placeholder="e.g. Federated Learning")
            add_short = af2.text_input("Short name / acronym", placeholder="e.g. FL")
            add_desc  = st.text_area("What it is *", height=120,
                placeholder="Describe the technology and why it matters for business model transformation…")
            add_thesis = st.text_area("Disruption thesis", height=100,
                placeholder="Which business models does this threaten or enable? Why now?")
            st.markdown("**Details**")
            bf1, bf2 = st.columns(2)
            add_cat_opts = ["AI/ML", "Infrastructure", "Data", "Hardware", "Fintech", "Biotech", "Other"]
            add_cat = bf1.selectbox("Category", add_cat_opts, key="add_cat")
            add_mat = bf2.slider("Maturity level (0–100)", 0, 100, 30, key="add_mat",
                                  help="0=research, 30=early pilots, 60=production-ready, 80=widespread, 100=commodity")
            add_uc  = st.text_area("Primary use cases (one per line)", height=90,
                placeholder="Enterprise knowledge management\nCustomer support automation\n…")
            add_kp  = st.text_area("Key players (one per line)", height=70,
                placeholder="Google DeepMind\nOpenAI\nAnthropix\n…")
            add_mat_rat = st.text_area("Maturity rationale / source", height=60,
                placeholder="e.g. 'Gartner Hype Cycle 2025 — Slope of Enlightenment'")

            if st.form_submit_button("➕ Add technology", type="primary"):
                if not add_name.strip() or not add_desc.strip():
                    st.error("Name and description are required.")
                else:
                    now   = datetime.now(timezone.utc).isoformat()
                    new_id = next_tech_id()
                    uc_list = [l.strip() for l in add_uc.split("\n") if l.strip()]
                    kp_list = [l.strip() for l in add_kp.split("\n") if l.strip()]
                    run_query("""
                        CREATE (t:Technology {
                            tech_id:            $tid,
                            name:               $name,
                            short_name:         $short,
                            category:           $cat,
                            description:        $desc,
                            disruption_thesis:  $thesis,
                            primary_use_cases:  $uc,
                            key_players:        $kp,
                            maturity_level:     $mat,
                            maturity_rationale: $mat_rat,
                            tracking_status:    'Active',
                            source:             'manual_entry',
                            created_by:         'editorial',
                            confidence:         0.8,
                            created_at:         $now,
                            updated_at:         $now
                        })
                    """, tid=new_id, name=add_name.strip(), short=add_short.strip(),
                        cat=add_cat, desc=add_desc.strip(), thesis=add_thesis.strip(),
                        uc=uc_list, kp=kp_list, mat=float(add_mat),
                        mat_rat=add_mat_rat.strip(), now=now)
                    tech_append({"tech_id": new_id, "name": add_name.strip(),
                                  "timestamp": now, "change_type": "created",
                                  "reason": "Manual entry via UI"})
                    st.success(f"✅ Added {add_name.strip()} ({new_id})")
                    st.rerun()

    with add_tab2:
        st.markdown("Paste an article, research paper excerpt, or upload a text/PDF file. Claude will extract a structured technology profile for you to review before saving.")

        extract_method = st.radio("Input method", ["Paste text", "Upload file"], horizontal=True, key="tech_extract_method")
        raw_text = ""
        if extract_method == "Paste text":
            raw_text = st.text_area("Paste content here", height=200, key="tech_paste",
                placeholder="Paste a blog post, research paper, product announcement, or any description of the technology…")
        else:
            uploaded = st.file_uploader("Upload file", type=["txt", "md", "pdf"],
                                         key="tech_upload", help="Plain text, Markdown, or PDF (first 4000 chars used)")
            if uploaded:
                try:
                    if uploaded.type == "application/pdf":
                        import io
                        raw_bytes = uploaded.read()
                        try:
                            import pdfplumber
                            with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
                                raw_text = "\n".join(p.extract_text() or "" for p in pdf.pages[:8])
                        except ImportError:
                            st.warning("pdfplumber not installed — trying raw text extraction.")
                            raw_text = raw_bytes.decode("utf-8", errors="ignore")
                    else:
                        raw_text = uploaded.read().decode("utf-8", errors="ignore")
                    st.caption(f"Loaded {len(raw_text)} characters.")
                except Exception as e:
                    st.error(f"Could not read file: {e}")

        if raw_text and st.button("🤖 Extract technology profile", key="tech_extract_btn"):
            with st.spinner("Calling Claude to extract technology profile…"):
                try:
                    _client = _anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
                    resp = _client.messages.create(
                        model="claude-haiku-4-5",
                        max_tokens=2000,
                        system="""You are a technology analyst. Extract a structured technology profile from the provided text.
Return ONLY a JSON object with these fields (use null for fields you cannot determine):
{
  "name": "Full technology name",
  "short_name": "Acronym or short name (or null)",
  "category": "One of: AI/ML, Infrastructure, Data, Hardware, Fintech, Biotech, Other",
  "description": "2-4 sentence description of what it is and how it works",
  "disruption_thesis": "2-3 sentences on which business models it threatens or enables and why",
  "primary_use_cases": ["use case 1", "use case 2", "use case 3"],
  "key_players": ["Company A", "Company B"],
  "maturity_level": <integer 0-100>,
  "maturity_rationale": "One sentence on why you assigned that maturity level"
}""",
                        messages=[{"role": "user", "content": f"Extract a technology profile from this text:\n\n{raw_text[:5000]}"}],
                    )
                    import re as _re
                    raw_resp = resp.content[0].text.strip()
                    m = _re.search(r"\{[\s\S]*\}", raw_resp)
                    if m:
                        extracted = json.loads(m.group(0))
                        st.session_state["tech_extracted"] = extracted
                        st.success("✅ Profile extracted — review and save below.")
                    else:
                        st.error("Could not parse JSON from response.")
                except Exception as e:
                    st.error(f"Extraction failed: {e}")

        if "tech_extracted" in st.session_state:
            ex = st.session_state["tech_extracted"]
            st.markdown("#### Review extracted profile")
            with st.form("tech_extracted_form"):
                ef1, ef2 = st.columns(2)
                ex_name  = ef1.text_input("Name", value=ex.get("name") or "")
                ex_short = ef2.text_input("Short name", value=ex.get("short_name") or "")
                ex_desc   = st.text_area("What it is", value=ex.get("description") or "", height=100)
                ex_thesis = st.text_area("Disruption thesis", value=ex.get("disruption_thesis") or "", height=90)
                ex_uc = st.text_area("Use cases (one per line)",
                                     value="\n".join(ex.get("primary_use_cases") or []), height=80)
                ex_kp = st.text_area("Key players (one per line)",
                                     value="\n".join(ex.get("key_players") or []), height=70)
                ef3, ef4 = st.columns(2)
                ex_cat_opts = ["AI/ML", "Infrastructure", "Data", "Hardware", "Fintech", "Biotech", "Other"]
                cur_cat = ex.get("category") or "Other"
                ex_cat = ef3.selectbox("Category", ex_cat_opts,
                    index=ex_cat_opts.index(cur_cat) if cur_cat in ex_cat_opts else len(ex_cat_opts)-1,
                    key="ex_cat_sel")
                ex_mat = ef4.slider("Maturity", 0, 100, int(ex.get("maturity_level") or 30), key="ex_mat_sl")
                ex_mat_rat = st.text_area("Maturity rationale", value=ex.get("maturity_rationale") or "", height=60)

                if st.form_submit_button("➕ Save this technology", type="primary"):
                    if not ex_name.strip():
                        st.error("Name is required.")
                    else:
                        now    = datetime.now(timezone.utc).isoformat()
                        new_id = next_tech_id()
                        uc_list = [l.strip() for l in ex_uc.split("\n") if l.strip()]
                        kp_list = [l.strip() for l in ex_kp.split("\n") if l.strip()]
                        run_query("""
                            CREATE (t:Technology {
                                tech_id:            $tid,
                                name:               $name,
                                short_name:         $short,
                                category:           $cat,
                                description:        $desc,
                                disruption_thesis:  $thesis,
                                primary_use_cases:  $uc,
                                key_players:        $kp,
                                maturity_level:     $mat,
                                maturity_rationale: $mat_rat,
                                tracking_status:    'Active',
                                source:             'extracted',
                                created_by:         'editorial_extract',
                                confidence:         0.75,
                                created_at:         $now,
                                updated_at:         $now
                            })
                        """, tid=new_id, name=ex_name.strip(), short=ex_short.strip(),
                            cat=ex_cat, desc=ex_desc.strip(), thesis=ex_thesis.strip(),
                            uc=uc_list, kp=kp_list, mat=float(ex_mat),
                            mat_rat=ex_mat_rat.strip(), now=now)
                        tech_append({"tech_id": new_id, "name": ex_name.strip(),
                                      "timestamp": now, "change_type": "created",
                                      "reason": "Extracted from uploaded content"})
                        del st.session_state["tech_extracted"]
                        st.success(f"✅ Added {ex_name.strip()} ({new_id})")
                        st.rerun()

    # ── Global changelog ────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📋 Technology Edit Log")
    if os.path.exists(tech_log_path):
        with open(tech_log_path) as f:
            all_tech_log = [json.loads(l) for l in f if l.strip()]
        if all_tech_log:
            st.dataframe([{
                "Time":    e.get("timestamp","")[:16],
                "Tech":    e.get("name","") or e.get("tech_id",""),
                "Change":  e.get("change_type",""),
                "Reason":  e.get("reason","")[:80],
            } for e in reversed(all_tech_log)], use_container_width=True, hide_index=True)
    else:
        st.caption("No changes logged yet.")


# ── Page: Companies ───────────────────────────────────────────────────────────

elif page == "🏢 Companies":
    st.title("🏢 Company Database")
    st.caption("Companies tagged to their business models, industries, and evidence in the graph.")

    # ── filters ──
    cf1, cf2, cf3, cf4 = st.columns([3, 2, 2, 1])
    with cf1:
        co_search = st.text_input("Search", placeholder="company name, description…",
                                  label_visibility="collapsed")
    with cf2:
        _bim_opts = run_query("MATCH (b:BusinessModel) RETURN b.bim_id AS id, b.name AS name ORDER BY b.bim_id") or []
        bim_filter_map = {"All business models": None}
        for b in _bim_opts:
            bim_filter_map[f"{b['id']}: {b['name']}"] = b["id"]
        co_bim_sel = st.selectbox("Business model", list(bim_filter_map.keys()),
                                  label_visibility="collapsed")
    with cf3:
        _ind_opts = run_query("""
            MATCH (c:Company) RETURN DISTINCT c.primary_industry AS ind ORDER BY ind
        """) or []
        industry_opts = ["All industries"] + [r["ind"] for r in _ind_opts if r.get("ind")]
        co_ind = st.selectbox("Industry", industry_opts, label_visibility="collapsed")
    with cf4:
        co_sort = st.selectbox("Sort", ["Name", "Revenue", "Evidence links"],
                               label_visibility="collapsed")

    # ── build query ──
    co_where = []
    co_params: dict = {}
    selected_bim = bim_filter_map[co_bim_sel]
    if selected_bim:
        co_where.append("EXISTS((c)-[:OPERATES_AS]->(:BusinessModel {bim_id: $bim_id}))")
        co_params["bim_id"] = selected_bim
    if co_ind != "All industries":
        co_where.append("c.primary_industry = $industry")
        co_params["industry"] = co_ind

    co_where_clause = ("WHERE " + " AND ".join(co_where)) if co_where else ""
    co_sort_clause = {
        "Name": "ORDER BY c.name",
        "Revenue": "ORDER BY c.revenue_range DESC, c.name",
        "Evidence links": "ORDER BY evidence_count DESC, c.name",
    }[co_sort]

    companies_data = run_query(f"""
        MATCH (c:Company)
        {co_where_clause}
        WITH c, size([(c)-[:HAS_EVIDENCE]->() | 1]) AS evidence_count
        OPTIONAL MATCH (c)-[r_bim:OPERATES_AS]->(b:BusinessModel)
        WITH c, evidence_count,
             collect({{bim_id: b.bim_id, name: b.name, is_primary: r_bim.is_primary}}) AS bims
        RETURN c.company_id       AS cid,
               c.name             AS name,
               c.ticker           AS ticker,
               c.description      AS description,
               c.primary_industry AS industry,
               c.secondary_industries AS secondary_industries,
               c.employee_range   AS employees,
               c.revenue_range    AS revenue,
               c.hq_country       AS country,
               c.updated_at       AS updated_at,
               evidence_count,
               bims
        {co_sort_clause}
    """, **co_params) or []

    if co_search:
        q = co_search.lower()
        companies_data = [c for c in companies_data if
                          q in (c.get("name") or "").lower() or
                          q in (c.get("description") or "").lower() or
                          q in (c.get("industry") or "").lower()]

    st.markdown(f"**{len(companies_data)} companies**")

    if not companies_data:
        st.info("No companies found. Adjust filters or import via scripts/import_companies.py")
    else:
        for co in companies_data:
            cid  = co["cid"]
            name = co["name"] or ""
            evn  = co.get("evidence_count") or 0
            bims = co.get("bims") or []
            primary_bim = next((b for b in bims if b.get("is_primary")), None)
            other_bims  = [b for b in bims if not b.get("is_primary")]

            bim_label = primary_bim["name"] if primary_bim else "—"
            ev_badge  = f"  ·  📎 {evn} case {'study' if evn == 1 else 'studies'}" if evn else ""

            with st.expander(f"**{name}**  ·  {co.get('ticker','') or ''}  ·  {bim_label}{ev_badge}"):
                left, right = st.columns([3, 1])

                with left:
                    st.markdown(co.get("description") or "_No description yet._")
                    inds = [co.get("industry") or ""] + (co.get("secondary_industries") or [])
                    inds = [i for i in inds if i]
                    st.caption(
                        f"🏭 {' · '.join(inds)}  |  "
                        f"👥 {co.get('employees','—')}  |  "
                        f"💰 {co.get('revenue','—')}  |  "
                        f"🌍 {co.get('country','USA')}"
                    )

                    # business models
                    st.markdown("**Business models**")
                    if bims:
                        bm_cols = st.columns(min(len(bims), 3))
                        for i, b in enumerate(bims):
                            tag = "🔵 Primary" if b.get("is_primary") else "⚪ Secondary"
                            bm_cols[i % 3].markdown(f"{tag}  \n`{b['bim_id']}` {b.get('name','')}")
                    else:
                        st.caption("No business models linked yet.")

                    # evidence
                    if evn:
                        st.divider()
                        st.markdown(f"**📎 Case studies ({evn})**")
                        _evidence = run_query("""
                            MATCH (co:Company {company_id: $cid})-[:HAS_EVIDENCE]->(e:Evidence)
                            MATCH (e)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
                            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                            RETURN e.transition_summary AS summary,
                                   e.source_url AS url,
                                   f.name AS from_bm, t.name AS to_bm
                            LIMIT 5
                        """, cid=cid) or []
                        for ev in _evidence:
                            st.markdown(
                                f"→ **{ev.get('from_bm','?')} → {ev.get('to_bm','?')}**  \n"
                                f"{ev.get('summary','')[:120]}"
                                + (f"  \n[Source]({ev['url']})" if ev.get("url") else "")
                            )

                    # disruption exposure — linked hypotheses
                    _hyp_links = run_query("""
                        MATCH (c:Company {company_id: $cid})-[:EXPOSED_TO]->(h:DisruptionHypothesis)
                        MATCH (h)-[:TRIGGERED_BY]->(tech:Technology)
                        OPTIONAL MATCH (to_bm:BusinessModel {bim_id: h.to_bim_id})
                        RETURN h.hypothesis_id AS hid,
                               h.title AS title,
                               h.conviction_score AS conviction,
                               h.disruption_type AS dtype,
                               h.time_horizon AS horizon,
                               tech.name AS tech_name,
                               to_bm.name AS to_bm
                        ORDER BY h.conviction_score DESC
                        LIMIT 5
                    """, cid=cid) or []
                    if _hyp_links:
                        st.divider()
                        st.markdown(f"**⚠️ Disruption hypotheses ({len(_hyp_links)})**")
                        for dh in _hyp_links:
                            conv  = dh.get("conviction") or 0
                            icon  = "🔴" if conv >= 0.7 else "🟡"
                            st.markdown(
                                f"{icon} → **{dh.get('to_bm','?')}**  "
                                f"·  ⚡ {dh.get('tech_name','?')}  "
                                f"·  conviction={conv:.2f}  "
                                f"·  {dh.get('dtype','?')}  "
                                f"·  {dh.get('horizon','?')}  \n"
                                f"<span style='font-size:0.8rem;color:#aaa'>{(dh.get('title') or '')[:80]}</span>",
                                unsafe_allow_html=True,
                            )

                with right:
                    st.metric("Ticker", co.get("ticker") or "—")
                    st.metric("Revenue", co.get("revenue") or "—")
                    st.metric("Employees", co.get("employees") or "—")
                    st.metric("Case studies", str(evn))

                    co_edit_key = f"co_edit_{cid}"
                    if co_edit_key not in st.session_state:
                        st.session_state[co_edit_key] = False
                    if st.button("✏️ Edit", key=f"co_edit_btn_{cid}"):
                        st.session_state[co_edit_key] = not st.session_state[co_edit_key]

                    if st.session_state[co_edit_key]:
                        with st.form(key=f"co_edit_form_{cid}"):
                            new_desc = st.text_area("Description",
                                                    value=co.get("description") or "", height=100)
                            new_ind  = st.text_input("Primary industry",
                                                     value=co.get("industry") or "")
                            new_rev  = st.selectbox("Revenue range",
                                ["<$100M","$100M-$500M","$500M-$2B","$2B+"],
                                index=["<$100M","$100M-$500M","$500M-$2B","$2B+"].index(
                                    co.get("revenue","<$100M"))
                                    if co.get("revenue") in ["<$100M","$100M-$500M","$500M-$2B","$2B+"] else 0)
                            if st.form_submit_button("💾 Save"):
                                now_str = datetime.now(timezone.utc).isoformat()
                                run_query("""
                                    MATCH (c:Company {company_id: $cid})
                                    SET c.description = $desc,
                                        c.primary_industry = $ind,
                                        c.revenue_range = $rev,
                                        c.updated_at = $now,
                                        c.edited_by = 'human_ui'
                                """, cid=cid, desc=new_desc, ind=new_ind,
                                    rev=new_rev, now=now_str)
                                try:
                                    os.makedirs("data", exist_ok=True)
                                    with open("data/company_changelog.jsonl", "a") as _f:
                                        _f.write(json.dumps({
                                            "timestamp": now_str, "company_id": cid,
                                            "name": name, "changes": {
                                                "description": new_desc,
                                                "industry": new_ind,
                                                "revenue": new_rev,
                                            }, "edited_by": "human_ui",
                                        }) + "\n")
                                except Exception:
                                    pass
                                st.success("Saved")
                                st.session_state[co_edit_key] = False
                                st.rerun()


# ── Page: Graph Overview ──────────────────────────────────────────────────────

if page == "📊 Graph Overview":
    st.title("Graph Overview")

    counts = run_query("""
        MATCH (n)
        RETURN labels(n)[0] AS label, count(n) AS cnt
        ORDER BY cnt DESC
    """)

    rel_counts = run_query("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel_type, count(r) AS cnt
        ORDER BY cnt DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Node counts")
        for row in counts:
            st.metric(row["label"], row["cnt"])

    with col2:
        st.subheader("Relationship counts")
        for row in rel_counts:
            st.metric(row["rel_type"], row["cnt"])


# ── Page: Input Review Queue ──────────────────────────────────────────────────

elif page == "📋 Input Review Queue":
    st.title("📋 Input Review Queue")
    st.caption("Every item created or found by agents must be reviewed here before it's considered active.")

    _now_irq = datetime.now(timezone.utc).isoformat()

    # ── queue counts ──────────────────────────────────────────────────────────
    _q_hyp  = run_query("MATCH (h:DisruptionHypothesis) WHERE h.pending_human_review = true OR h.status = 'Hypothesis' RETURN count(h) AS n") or [{"n":0}]
    _q_ev   = run_query("MATCH (e:Evidence) WHERE e.reviewed_by IS NULL RETURN count(e) AS n") or [{"n":0}]
    _q_tv   = run_query("MATCH (v:TransformationVector) WHERE v.reviewed_by IS NULL AND v.created_by = 'vector_extractor' RETURN count(v) AS n") or [{"n":0}]
    _q_sc   = run_query("MATCH (s:Scalar) WHERE s.pending_human_review = true RETURN count(s) AS n") or [{"n":0}]
    _q_tech = run_query("MATCH (t:Technology) WHERE t.pending_human_review = true RETURN count(t) AS n") or [{"n":0}]
    _q_bm   = run_query("MATCH (b:BusinessModel) WHERE b.pending_human_review = true RETURN count(b) AS n") or [{"n":0}]
    _q_co   = run_query("MATCH (c:Company) WHERE c.pending_human_review = true RETURN count(c) AS n") or [{"n":0}]

    n_hyp  = _q_hyp[0]["n"]
    n_ev   = _q_ev[0]["n"]
    n_tv   = _q_tv[0]["n"]
    n_sc   = _q_sc[0]["n"]
    n_tech = _q_tech[0]["n"]
    n_bm   = _q_bm[0]["n"]
    n_co   = _q_co[0]["n"]
    total_pending = n_hyp + n_ev + n_tv + n_sc + n_tech + n_bm + n_co

    # summary banner
    badge_color = "#c0392b" if total_pending > 0 else "#27ae60"
    st.markdown(
        f"<div style='background:#1a1a1a;border:1px solid {badge_color};border-radius:8px;"
        f"padding:12px 18px;margin-bottom:16px;display:flex;gap:24px;flex-wrap:wrap'>"
        f"<span style='color:{badge_color};font-weight:700;font-size:1.1rem'>🔔 {total_pending} pending</span>"
        f"<span style='color:#aaa'>🧠 {n_hyp} hypotheses</span>"
        f"<span style='color:#aaa'>📎 {n_ev} case studies</span>"
        f"<span style='color:#aaa'>🔀 {n_tv} transformations</span>"
        f"<span style='color:#aaa'>⚡ {n_sc} scalars</span>"
        f"<span style='color:#aaa'>🔬 {n_tech} technologies</span>"
        f"<span style='color:#aaa'>📚 {n_bm} business models</span>"
        f"<span style='color:#aaa'>🏢 {n_co} companies</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    tab_hyp, tab_ev, tab_tv, tab_sc, tab_tech, tab_bm, tab_co = st.tabs([
        f"🧠 Hypotheses ({n_hyp})",
        f"📎 Case Studies ({n_ev})",
        f"🔀 Transformations ({n_tv})",
        f"⚡ Scalars ({n_sc})",
        f"🔬 Technologies ({n_tech})",
        f"📚 Business Models ({n_bm})",
        f"🏢 Companies ({n_co})",
    ])

    # ── helper ─────────────────────────────────────────────────────────────────
    def irq_approve_reject(node_label, id_field, node_id, tab_prefix,
                           approve_cypher, reject_cypher,
                           approve_params=None, reject_params=None):
        """Render approve / reject buttons. Returns nothing; triggers st.rerun on action."""
        c1, c2 = st.columns(2)
        if c1.button("✅ Approve", key=f"irq_approve_{tab_prefix}_{node_id}"):
            p = (approve_params or {})
            p.setdefault("now", _now_irq)
            p[id_field] = node_id
            run_query(approve_cypher, **p)
            st.rerun()
        if c2.button("❌ Reject", key=f"irq_reject_{tab_prefix}_{node_id}"):
            p = (reject_params or {})
            p.setdefault("now", _now_irq)
            p[id_field] = node_id
            run_query(reject_cypher, **p)
            st.rerun()

    # ── Hypotheses ─────────────────────────────────────────────────────────────
    with tab_hyp:
        if n_hyp == 0:
            st.success("✅ All hypotheses reviewed.")
        else:
            _rq_page_hyp = st.number_input("Page", min_value=1, value=1, key="irq_page_hyp", step=1)
            _rq_ps_hyp = 10
            _rq_skip_hyp = (_rq_page_hyp - 1) * _rq_ps_hyp

            hyps = run_query(f"""
                MATCH (h:DisruptionHypothesis)-[:TRIGGERED_BY]->(t:Technology)
                MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
                OPTIONAL MATCH (fb:BusinessModel {{bim_id: h.from_bim_id}})
                OPTIONAL MATCH (tb:BusinessModel {{bim_id: h.to_bim_id}})
                WHERE h.pending_human_review = true OR h.status = 'Hypothesis'
                RETURN h.hypothesis_id AS hid,
                       h.title          AS title,
                       h.thesis         AS thesis,
                       h.conviction_score AS conviction,
                       h.activation_score AS activation,
                       h.disruption_type  AS dtype,
                       h.time_horizon     AS horizon,
                       h.companies_exposed AS companies,
                       t.name AS tech_name,
                       fb.name AS from_bm, tb.name AS to_bm,
                       h.created_at AS created_at
                ORDER BY h.conviction_score DESC
                SKIP {_rq_skip_hyp} LIMIT {_rq_ps_hyp}
            """) or []

            st.caption(f"Showing {_rq_skip_hyp+1}–{_rq_skip_hyp+len(hyps)} of {n_hyp}")

            for h in hyps:
                hid  = h["hid"]
                conv = h.get("conviction") or 0
                act  = h.get("activation") or 0
                icon = "🟢" if conv >= 0.7 else ("🟡" if conv >= 0.5 else "🔴")
                with st.expander(
                    f"{icon}  **{h.get('from_bm','?')} → {h.get('to_bm','?')}**  "
                    f"·  ⚡ {h.get('tech_name','?')}  ·  conviction={conv:.2f}  ·  activation={act:.3f}"
                ):
                    st.markdown(f"**{h.get('title','—')}**")
                    st.markdown(h.get("thesis") or "—")
                    cos_str = ", ".join(h.get("companies") or []) or "—"
                    st.caption(f"Companies mentioned: {cos_str[:120]}")
                    st.caption(f"Type: {h.get('dtype','?')}  ·  Horizon: {h.get('horizon','?')}  ·  ID: {hid}")

                    # linked companies from DB
                    _lc = run_query("""
                        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
                        RETURN c.name AS name, c.ticker AS ticker
                        ORDER BY c.name LIMIT 8
                    """, hid=hid) or []
                    if _lc:
                        st.caption("🏢 In our database: " + "  ·  ".join(
                            f"{r['name']} ({r['ticker']})" if r.get('ticker') else r['name']
                            for r in _lc
                        ))

                    st.divider()
                    a1, a2, a3, a4 = st.columns(4)
                    if a1.button("✅ Validate", key=f"irq_hyp_val_{hid}"):
                        run_query("""
                            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                            SET h.status = 'Validated',
                                h.pending_human_review = false,
                                h.reviewed_at = $now, h.reviewed_by = 'human_ui'
                        """, hid=hid, now=_now_irq)
                        st.rerun()
                    if a2.button("❌ Reject", key=f"irq_hyp_rej_{hid}"):
                        run_query("""
                            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                            SET h.status = 'Rejected',
                                h.pending_human_review = false,
                                h.reviewed_at = $now, h.reviewed_by = 'human_ui'
                        """, hid=hid, now=_now_irq)
                        st.rerun()
                    if a3.button("⬆️ Escalate", key=f"irq_hyp_esc_{hid}"):
                        run_query("""
                            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                            SET h.status = 'Escalated',
                                h.pending_human_review = false,
                                h.reviewed_at = $now, h.reviewed_by = 'human_ui'
                        """, hid=hid, now=_now_irq)
                        st.rerun()
                    if a4.button("🔬 More research", key=f"irq_hyp_res_{hid}"):
                        run_query("""
                            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                            SET h.status = 'Needs Research',
                                h.pending_human_review = true,
                                h.reviewed_at = $now, h.reviewed_by = 'human_ui'
                        """, hid=hid, now=_now_irq)
                        st.rerun()

            # bulk approve
            st.divider()
            if st.button(f"✅✅ Approve all {n_hyp} hypotheses", key="irq_hyp_bulk"):
                run_query("""
                    MATCH (h:DisruptionHypothesis)
                    WHERE h.pending_human_review = true OR h.status = 'Hypothesis'
                    SET h.status = 'Validated',
                        h.pending_human_review = false,
                        h.reviewed_at = $now, h.reviewed_by = 'human_ui_bulk'
                """, now=_now_irq)
                st.rerun()

    # ── Case Studies (Evidence) ─────────────────────────────────────────────────
    with tab_ev:
        if n_ev == 0:
            st.success("✅ All case studies reviewed.")
        else:
            _rq_page_ev = st.number_input("Page", min_value=1, value=1, key="irq_page_ev", step=1)
            _rq_ps_ev   = 15
            _rq_skip_ev = (_rq_page_ev - 1) * _rq_ps_ev

            evs = run_query(f"""
                MATCH (e:Evidence)
                WHERE e.reviewed_by IS NULL
                OPTIONAL MATCH (e)-[:SUPPORTS]->(v:TransformationVector)
                                -[:FROM_BIM]->(fb:BusinessModel)
                OPTIONAL MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
                WITH e, head(collect(fb.name)) AS from_bm,
                         head(collect(tb.name)) AS to_bm
                RETURN e.evidence_id     AS eid,
                       e.transition_summary AS summary,
                       e.source_url      AS url,
                       e.source_type     AS source_type,
                       e.confidence      AS confidence,
                       e.companies_mentioned AS companies,
                       e.evidence_quote  AS quote,
                       e.extracted_at    AS extracted_at,
                       e.created_by      AS created_by,
                       from_bm, to_bm
                ORDER BY e.extracted_at DESC
                SKIP {_rq_skip_ev} LIMIT {_rq_ps_ev}
            """) or []

            st.caption(f"Showing {_rq_skip_ev+1}–{_rq_skip_ev+len(evs)} of {n_ev}")

            for ev in evs:
                eid  = ev["eid"]
                conf = ev.get("confidence") or 0
                bm_label = f"{ev.get('from_bm','?')} → {ev.get('to_bm','?')}" if ev.get("from_bm") else "—"
                with st.expander(
                    f"**{eid}**  ·  {bm_label}  ·  conf={conf:.2f}"
                    + (f"  ·  [{ev.get('source_type','')}]" if ev.get("source_type") else "")
                ):
                    st.markdown(ev.get("summary") or "_No summary_")
                    if ev.get("quote"):
                        st.markdown(
                            f"<blockquote style='border-left:3px solid #555;padding:6px 10px;"
                            f"color:#aaa;font-size:0.85rem'>{(ev['quote'] or '')[:300]}</blockquote>",
                            unsafe_allow_html=True,
                        )
                    cos = ", ".join(ev.get("companies") or []) or "—"
                    st.caption(f"Companies: {cos}")
                    if ev.get("url"):
                        st.caption(f"[Source]({ev['url']})")
                    st.caption(f"Extracted: {ev.get('extracted_at','')}  ·  By: {ev.get('created_by','?')}")

                    st.divider()
                    c1, c2, c3 = st.columns(3)
                    if c1.button("✅ Approve", key=f"irq_ev_app_{eid}"):
                        run_query("""
                            MATCH (e:Evidence {evidence_id: $eid})
                            SET e.reviewed_by = 'human_ui',
                                e.reviewed_at = $now
                        """, eid=eid, now=_now_irq)
                        st.rerun()
                    if c2.button("❌ Reject", key=f"irq_ev_rej_{eid}"):
                        run_query("""
                            MATCH (e:Evidence {evidence_id: $eid})
                            SET e.reviewed_by = 'human_ui',
                                e.reviewed_at = $now,
                                e.status = 'Rejected'
                        """, eid=eid, now=_now_irq)
                        st.rerun()
                    if c3.button("🚩 Flag", key=f"irq_ev_flag_{eid}"):
                        run_query("""
                            MATCH (e:Evidence {evidence_id: $eid})
                            SET e.reviewed_by = 'human_ui',
                                e.reviewed_at = $now,
                                e.status = 'Flagged'
                        """, eid=eid, now=_now_irq)
                        st.rerun()

            st.divider()
            if st.button(f"✅✅ Approve all {n_ev} case studies", key="irq_ev_bulk"):
                run_query("""
                    MATCH (e:Evidence) WHERE e.reviewed_by IS NULL
                    SET e.reviewed_by = 'human_ui_bulk', e.reviewed_at = $now
                """, now=_now_irq)
                st.rerun()

    # ── Transformations (TransformationVector) ─────────────────────────────────
    with tab_tv:
        if n_tv == 0:
            st.success("✅ All agent-created transformations reviewed.")
        else:
            _rq_page_tv = st.number_input("Page", min_value=1, value=1, key="irq_page_tv", step=1)
            _rq_ps_tv   = 15
            _rq_skip_tv = (_rq_page_tv - 1) * _rq_ps_tv

            tvs = run_query(f"""
                MATCH (v:TransformationVector)
                WHERE v.reviewed_by IS NULL AND v.created_by = 'vector_extractor'
                MATCH (v)-[:FROM_BIM]->(fb:BusinessModel)
                MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
                WITH v, fb, tb,
                     size([(v)-[:SUPPORTS]-() | 1]) AS evidence_count,
                     size([(v)-[:IMPACTS]->() | 1]) AS scalar_count
                RETURN v.vector_id      AS vid,
                       v.example_text   AS example,
                       v.confidence     AS confidence,
                       v.signal_strength AS signal,
                       v.created_at     AS created_at,
                       evidence_count, scalar_count,
                       fb.bim_id AS from_id, fb.name AS from_bm,
                       tb.bim_id AS to_id,   tb.name AS to_bm
                ORDER BY v.created_at DESC
                SKIP {_rq_skip_tv} LIMIT {_rq_ps_tv}
            """) or []

            st.caption(f"Showing {_rq_skip_tv+1}–{_rq_skip_tv+len(tvs)} of {n_tv} agent-created transformations")

            for tv in tvs:
                vid  = tv["vid"]
                conf = tv.get("confidence") or 0
                sig  = tv.get("signal") or 0
                with st.expander(
                    f"**{vid}**  ·  {tv.get('from_bm','?')} → {tv.get('to_bm','?')}  "
                    f"·  conf={conf:.2f}  ·  signal={sig:.2f}  "
                    f"·  {tv.get('evidence_count',0)} evidence  ·  {tv.get('scalar_count',0)} scalars"
                ):
                    if tv.get("example"):
                        st.markdown(f"_{tv['example'][:300]}_")
                    st.caption(
                        f"{tv.get('from_id','')} → {tv.get('to_id','')}  ·  "
                        f"Created: {tv.get('created_at','')}"
                    )
                    st.divider()
                    c1, c2 = st.columns(2)
                    if c1.button("✅ Approve", key=f"irq_tv_app_{vid}"):
                        run_query("""
                            MATCH (v:TransformationVector {vector_id: $vid})
                            SET v.reviewed_by = 'human_ui', v.reviewed_at = $now
                        """, vid=vid, now=_now_irq)
                        st.rerun()
                    if c2.button("❌ Reject", key=f"irq_tv_rej_{vid}"):
                        run_query("""
                            MATCH (v:TransformationVector {vector_id: $vid})
                            SET v.reviewed_by = 'human_ui', v.reviewed_at = $now,
                                v.status = 'Rejected'
                        """, vid=vid, now=_now_irq)
                        st.rerun()

            st.divider()
            if st.button(f"✅✅ Approve all {n_tv} transformations", key="irq_tv_bulk"):
                run_query("""
                    MATCH (v:TransformationVector)
                    WHERE v.reviewed_by IS NULL AND v.created_by = 'vector_extractor'
                    SET v.reviewed_by = 'human_ui_bulk', v.reviewed_at = $now
                """, now=_now_irq)
                st.rerun()

    # ── Scalars ────────────────────────────────────────────────────────────────
    with tab_sc:
        scs = run_query("""
            MATCH (s:Scalar)
            WHERE s.pending_human_review = true
            RETURN s.scalar_id AS id, s.name AS name, s.group AS grp,
                   s.description AS description, s.trend_direction AS trend,
                   s.confidence AS confidence, s.created_at AS created_at
            ORDER BY s.created_at DESC
        """) or []
        if not scs:
            st.success("✅ No scalars pending review.")
        else:
            for sc in scs:
                sid = sc["id"]
                with st.expander(f"**{sid}** — {sc['name']}  ·  group={sc.get('grp','?')}"):
                    st.markdown(sc.get("description") or "—")
                    st.caption(f"Trend: {sc.get('trend','?')}  ·  Confidence: {sc.get('confidence','?')}  ·  Created: {sc.get('created_at','')}")
                    st.divider()
                    irq_approve_reject(
                        "Scalar", "id", sid, "sc",
                        "MATCH (s:Scalar {scalar_id: $id}) SET s.pending_human_review=false, s.status='Active', s.reviewed_at=$now, s.reviewed_by='human_ui'",
                        "MATCH (s:Scalar {scalar_id: $id}) SET s.pending_human_review=false, s.status='Rejected', s.reviewed_at=$now, s.reviewed_by='human_ui'",
                    )

    # ── Technologies ───────────────────────────────────────────────────────────
    with tab_tech:
        techs = run_query("""
            MATCH (n:Technology)
            WHERE n.pending_human_review = true
            WITH n, size([(n)-[:MOVES_SCALAR]->() | 1]) AS scalar_count,
                 size([(n)-[:ACTIVATES]->() | 1]) AS vector_count
            RETURN n.tech_id AS id, n.name AS name,
                   n.maturity_level AS maturity,
                   n.description AS description,
                   n.disruption_thesis AS thesis,
                   scalar_count, vector_count,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """) or []
        if not techs:
            st.success("✅ No technologies pending review.")
        else:
            for tech in techs:
                tid = tech["id"]
                with st.expander(
                    f"**{tid}** — {tech['name']}  ·  "
                    f"maturity={tech.get('maturity','?')}  ·  "
                    f"{tech.get('scalar_count',0)} scalars  ·  {tech.get('vector_count',0)} vectors activated"
                ):
                    st.markdown(tech.get("description") or "—")
                    if tech.get("thesis"):
                        st.markdown(f"**Thesis:** {tech['thesis']}")
                    st.caption(f"Created: {tech.get('created_at','')}")
                    st.divider()
                    irq_approve_reject(
                        "Technology", "id", tid, "tech",
                        "MATCH (n:Technology {tech_id: $id}) SET n.pending_human_review=false, n.reviewed_at=$now, n.reviewed_by='human_ui'",
                        "MATCH (n:Technology {tech_id: $id}) SET n.pending_human_review=false, n.tracking_status='Rejected', n.reviewed_at=$now, n.reviewed_by='human_ui'",
                    )

    # ── Business Models ────────────────────────────────────────────────────────
    with tab_bm:
        bms = run_query("""
            MATCH (n:BusinessModel)
            WHERE n.pending_human_review = true
            RETURN n.bim_id AS id, n.name AS name, n.source AS source,
                   n.description AS description,
                   n.typical_margins AS margins,
                   coalesce(n.confidence, 0.0) AS confidence,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """) or []
        if not bms:
            st.success("✅ No business models pending review.")
        else:
            for bm in bms:
                bid = bm["id"]
                with st.expander(f"**{bid}** — {bm['name']}  ·  conf={bm['confidence']:.2f}  ·  source={bm.get('source','?')}"):
                    st.markdown(bm.get("description") or "—")
                    st.caption(f"Margins: {bm.get('margins','?')}  ·  Created: {bm.get('created_at','')}")
                    st.divider()
                    irq_approve_reject(
                        "BusinessModel", "id", bid, "bm",
                        "MATCH (n:BusinessModel {bim_id: $id}) SET n.pending_human_review=false, n.reviewed_at=$now, n.reviewed_by='human_ui'",
                        "MATCH (n:BusinessModel {bim_id: $id}) SET n.status='Rejected', n.pending_human_review=false, n.reviewed_at=$now, n.reviewed_by='human_ui'",
                    )

    # ── Companies ──────────────────────────────────────────────────────────────
    with tab_co:
        companies = run_query("""
            MATCH (n:Company)
            WHERE n.pending_human_review = true
            OPTIONAL MATCH (n)-[:OPERATES_AS {is_primary: true}]->(bm:BusinessModel)
            RETURN n.company_id AS id, n.name AS name,
                   n.hq_country AS country,
                   n.description AS description,
                   n.primary_industry AS industry,
                   n.revenue_range AS revenue,
                   bm.bim_id AS bim_id, bm.name AS bm_name,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """) or []
        if not companies:
            st.success("✅ No companies pending review.")
        else:
            for co in companies:
                coid = co["id"]
                bm_label = f"{co.get('bim_id','?')} {co.get('bm_name','')}" if co.get("bim_id") else "No BM linked"
                with st.expander(f"**{coid}** — {co['name']}  ·  {bm_label}"):
                    st.markdown(co.get("description") or "—")
                    st.caption(
                        f"Industry: {co.get('industry','?')}  ·  "
                        f"Revenue: {co.get('revenue','?')}  ·  "
                        f"Country: {co.get('country','?')}  ·  "
                        f"Created: {co.get('created_at','')}"
                    )
                    st.divider()
                    irq_approve_reject(
                        "Company", "id", coid, "co",
                        "MATCH (n:Company {company_id: $id}) SET n.pending_human_review=false, n.reviewed_at=$now, n.reviewed_by='human_ui'",
                        "MATCH (n:Company {company_id: $id}) SET n.pending_human_review=false, n.status='Rejected', n.reviewed_at=$now, n.reviewed_by='human_ui'",
                    )


# ── Page: Hypotheses ──────────────────────────────────────────────────────────

elif page == "🧠 Hypotheses":
    st.title("Disruption Hypotheses")
    st.caption("Each hypothesis follows the causal chain: **Technology → Scalars → Transition**. "
               "Review the reasoning, give feedback, approve or reject.")

    # ── filters ──
    fcol1, fcol2, fcol3, fcol4 = st.columns([2, 2, 2, 1])
    with fcol1:
        search_hyp = st.text_input("Search", placeholder="title, thesis, company…", label_visibility="collapsed")
    with fcol2:
        tech_filter_opts = ["All technologies"]
        _tech_names = run_query("MATCH (t:Technology) RETURN t.tech_id AS id, t.name AS name ORDER BY t.tech_id")
        tech_id_map = {t["name"]: t["id"] for t in (_tech_names or [])}
        tech_filter_opts += list(tech_id_map.keys())
        selected_tech_name = st.selectbox("Technology", tech_filter_opts, label_visibility="collapsed")
    with fcol3:
        status_filter = st.selectbox("Status", ["All", "Pending review", "Validated", "Rejected", "Escalated"],
                                     label_visibility="collapsed")
    with fcol4:
        sort_hyp = st.selectbox("Sort", ["conviction_score", "activation_score", "created_at"],
                                label_visibility="collapsed")

    # ── query ──
    hyp_where = []
    hyp_params: dict = {}
    if selected_tech_name != "All technologies":
        hyp_where.append("t.tech_id = $tid")
        hyp_params["tid"] = tech_id_map[selected_tech_name]
    if status_filter == "Pending review":
        hyp_where.append("h.pending_human_review = true")
    elif status_filter != "All":
        hyp_where.append("h.status = $hstatus")
        hyp_params["hstatus"] = status_filter

    where_clause = ("WHERE " + " AND ".join(hyp_where)) if hyp_where else ""

    hypotheses = run_query(f"""
        MATCH (h:DisruptionHypothesis)-[:TRIGGERED_BY]->(t:Technology)
        MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (f:BusinessModel {{bim_id: h.from_bim_id}})
        OPTIONAL MATCH (tb:BusinessModel {{bim_id: h.to_bim_id}})
        {where_clause}
        RETURN h.hypothesis_id        AS hid,
               h.title                AS title,
               h.thesis               AS thesis,
               h.counter_argument     AS counter,
               h.conviction_score     AS conviction,
               h.activation_score     AS activation,
               h.disruption_type      AS dtype,
               h.time_horizon         AS horizon,
               h.primary_scalar_driver AS primary_scalar,
               h.supporting_scalars   AS supporting_scalars,
               h.companies_exposed    AS companies,
               h.evidence_count       AS evidence_count,
               h.status               AS status,
               h.pending_human_review AS pending,
               h.feedback             AS feedback,
               h.created_at           AS created_at,
               h.updated_at           AS updated_at,
               t.tech_id              AS tech_id,
               t.name                 AS tech_name,
               t.primary_scalar_driver AS tech_primary_scalar,
               f.name AS from_name,
               tb.name AS to_name,
               v.vector_id            AS vid
        ORDER BY h.{sort_hyp} DESC
    """, **hyp_params)

    all_hyps = hypotheses or []

    if search_hyp:
        q = search_hyp.lower()
        all_hyps = [h for h in all_hyps if
                    q in (h.get("title") or "").lower() or
                    q in (h.get("thesis") or "").lower() or
                    q in " ".join(h.get("companies") or []).lower() or
                    q in (h.get("tech_name") or "").lower()]

    pending_count = sum(1 for h in all_hyps if h.get("pending"))
    st.markdown(f"**{len(all_hyps)} hypotheses** · 🔔 {pending_count} pending review")

    # ── load scalar name lookup ──
    _scalar_rows = run_query("MATCH (s:Scalar) RETURN s.scalar_id AS id, s.name AS name")
    scalar_names = {r["id"]: r["name"] for r in (_scalar_rows or [])}

    # ── hypothesis cards ──
    for h in all_hyps:
        hid       = h["hid"]
        conv      = h.get("conviction") or 0
        activ     = h.get("activation")
        conv_icon = "🟢" if conv >= 0.7 else ("🟡" if conv >= 0.5 else "🔴")
        status    = h.get("status") or "Hypothesis"

        status_badge = {"Validated": "✅", "Rejected": "❌", "Escalated": "⬆️",
                        "Hypothesis": "🔔"}.get(status, "●")

        header = (
            f"{conv_icon} {status_badge}  **{h.get('from_name','?')} → {h.get('to_name','?')}**  "
            f"·  ⚡ {h.get('tech_name','?')}  ·  "
            f"conviction={conv:.2f}"
            + (f"  ·  activation={activ:.3f}" if activ is not None else "")
            + f"  ·  {h.get('dtype','?')}  ·  {h.get('horizon','?')}"
        )

        with st.expander(header):
            left_col, right_col = st.columns([3, 1])

            with left_col:
                # ── causal chain banner ──
                aligned_scalars = h.get("supporting_scalars") or []
                primary = h.get("primary_scalar") or ""
                scalar_chain = []
                if primary:
                    pname = scalar_names.get(primary, primary)
                    scalar_chain.append(f"**{primary}** {pname[:45]}")
                for sid in (aligned_scalars or []):
                    if sid != primary:
                        sname = scalar_names.get(sid, sid)
                        scalar_chain.append(f"{sid} {sname[:40]}")

                st.markdown(
                    f"<div style='background:#1a1a2e;border-left:3px solid #4a9eff;"
                    f"padding:10px 14px;border-radius:4px;margin-bottom:12px;font-size:0.88rem'>"
                    f"<span style='color:#4a9eff;font-weight:600'>⚡ {h.get('tech_name','')}</span>"
                    f"<span style='color:#888'> → moves scalars → </span>"
                    f"<span style='color:#7ec8e3'>{' · '.join(scalar_chain[:4]) or '—'}</span>"
                    f"<span style='color:#888'> → activates </span>"
                    f"<span style='color:#90ee90'>{h.get('from_name','?')} → {h.get('to_name','?')}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # ── thesis ──
                st.markdown("**Thesis**")
                st.markdown(h.get("thesis") or "—")

                # ── counter ──
                if h.get("counter"):
                    st.markdown("**Counter-argument**")
                    st.markdown(f"*{h['counter']}*")

                st.divider()

                # ── scalar movements detail ──
                if h.get("vid"):
                    _mv = run_query("""
                        MATCH (t:Technology {tech_id: $tid})-[r:MOVES_SCALAR]->(s:Scalar)
                        OPTIONAL MATCH (v:TransformationVector {vector_id: $vid})-[imp:IMPACTS]->(s)
                        RETURN s.scalar_id AS sid, s.name AS sname,
                               r.direction AS tech_dir, r.strength AS tech_str,
                               imp.direction AS vec_dir, imp.impact_score AS vec_score
                        ORDER BY abs(r.score) DESC
                    """, tid=h.get("tech_id"), vid=h.get("vid")) or []

                    if _mv:
                        st.markdown("**Scalar movements driving this transition**")
                        _mv_cols = st.columns([1, 4, 2, 2, 1])
                        _mv_cols[0].caption("Scalar")
                        _mv_cols[1].caption("Name")
                        _mv_cols[2].caption("Tech moves")
                        _mv_cols[3].caption("Vector needs")
                        _mv_cols[4].caption("Aligned?")
                        for mv in _mv:
                            aligned = mv.get("vec_dir") and mv.get("tech_dir") == mv.get("vec_dir")
                            _mv_cols[0].markdown(f"`{mv['sid']}`")
                            _mv_cols[1].markdown(mv.get("sname","")[:55])
                            _mv_cols[2].markdown(f"{mv.get('tech_dir','')} ({mv.get('tech_str','')})")
                            _mv_cols[3].markdown(f"{mv.get('vec_dir','—')}")
                            _mv_cols[4].markdown("✅" if aligned else ("❌" if mv.get("vec_dir") else "—"))

                st.divider()

                # ── metrics ──
                mc = st.columns(5)
                mc[0].metric("Conviction", f"{conv:.2f}")
                if activ is not None:
                    mc[1].metric("Activation", f"{activ:.3f}")
                mc[2].metric("Evidence", str(h.get("evidence_count") or 0))
                mc[3].metric("Type", h.get("dtype") or "—")
                mc[4].metric("Horizon", h.get("horizon") or "—")

                # ── companies in our database exposed to this hypothesis ──
                _linked_cos = run_query("""
                    MATCH (c:Company)-[r:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
                    OPTIONAL MATCH (c)-[:OPERATES_AS {is_primary: true}]->(b:BusinessModel)
                    RETURN c.company_id AS cid, c.name AS name, c.ticker AS ticker,
                           c.revenue_range AS rev, b.name AS bm,
                           r.reason AS reason
                    ORDER BY c.name
                """, hid=hid) or []

                if _linked_cos:
                    st.markdown(f"**🏢 Companies in our database exposed ({len(_linked_cos)})**")
                    # Display as compact grid of badges
                    _badge_rows = [_linked_cos[i:i+3] for i in range(0, len(_linked_cos), 3)]
                    for _row in _badge_rows:
                        _bcols = st.columns(3)
                        for _ci, _co in enumerate(_row):
                            _ticker = f" `{_co['ticker']}`" if _co.get("ticker") else ""
                            _rev    = f"  ·  {_co['rev']}" if _co.get("rev") else ""
                            _bcols[_ci].markdown(
                                f"**{_co['name']}**{_ticker}  \n"
                                f"<span style='font-size:0.8rem;color:#888'>{_co.get('bm','')[:35]}{_rev}</span>",
                                unsafe_allow_html=True,
                            )

                companies_str = ", ".join(h.get("companies") or []) or "—"
                st.caption(f"**Mentioned in thesis:** {companies_str}")
                st.caption(f"ID: {hid}  ·  Status: {status}  ·  Created: {h.get('created_at','')}")

                # ── existing feedback ──
                if h.get("feedback"):
                    st.markdown(
                        f"<div style='background:#2a1a0e;border-left:3px solid #ff9944;"
                        f"padding:8px 12px;border-radius:4px;margin-top:8px;font-size:0.88rem'>"
                        f"<b>📝 Feedback on file:</b> {h['feedback']}</div>",
                        unsafe_allow_html=True,
                    )

                # ── edit form ──
                edit_key = f"hyp_edit_open_{hid}"
                if edit_key not in st.session_state:
                    st.session_state[edit_key] = False
                if st.button("✏️ Edit hypothesis", key=f"hyp_edit_btn_{hid}"):
                    st.session_state[edit_key] = not st.session_state[edit_key]

                if st.session_state[edit_key]:
                    with st.form(key=f"hyp_edit_form_{hid}"):
                        st.markdown("**Edit Hypothesis**")
                        new_title     = st.text_input("Title", value=h.get("title") or "", key=f"htitle_{hid}")
                        new_thesis    = st.text_area("Thesis", value=h.get("thesis") or "", height=160, key=f"hthesis_{hid}")
                        new_counter   = st.text_area("Counter-argument", value=h.get("counter") or "", height=80, key=f"hcounter_{hid}")
                        new_companies_str = st.text_input(
                            "Companies exposed (comma-separated)",
                            value=", ".join(h.get("companies") or []),
                            key=f"hcompanies_{hid}"
                        )
                        ec1, ec2, ec3 = st.columns(3)
                        with ec1:
                            new_dtype = st.selectbox("Disruption type",
                                ["substitution","compression","unbundling","bundling","platform_shift","commoditisation"],
                                index=["substitution","compression","unbundling","bundling","platform_shift","commoditisation"].index(h.get("dtype") or "substitution")
                                    if h.get("dtype") in ["substitution","compression","unbundling","bundling","platform_shift","commoditisation"] else 0,
                                key=f"hdtype_{hid}")
                        with ec2:
                            new_horizon = st.selectbox("Time horizon",
                                ["0-2 years", "2-5 years", "5+ years"],
                                index=["0-2 years","2-5 years","5+ years"].index(h.get("horizon") or "2-5 years")
                                    if h.get("horizon") in ["0-2 years","2-5 years","5+ years"] else 1,
                                key=f"hhorizon_{hid}")
                        with ec3:
                            new_conviction = st.slider("Conviction", 0.0, 1.0,
                                float(h.get("conviction") or 0.5), 0.05, key=f"hconv_{hid}")
                        new_feedback = st.text_area(
                            "📝 Feedback / reasoning for this edit",
                            placeholder="Why are you changing this? What evidence or instinct is driving it?",
                            height=80, key=f"hfeedback_{hid}"
                        )
                        if st.form_submit_button("💾 Save changes"):
                            now_str = datetime.now(timezone.utc).isoformat()
                            new_companies = [c.strip() for c in new_companies_str.split(",") if c.strip()]
                            run_query("""
                                MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                                SET h.title           = $title,
                                    h.thesis          = $thesis,
                                    h.counter_argument = $counter,
                                    h.companies_exposed = $companies,
                                    h.disruption_type = $dtype,
                                    h.time_horizon    = $horizon,
                                    h.conviction_score = $conviction,
                                    h.feedback        = $feedback,
                                    h.updated_at      = $now,
                                    h.edited_by       = 'human_ui'
                            """, hid=hid, title=new_title, thesis=new_thesis,
                                counter=new_counter, companies=new_companies,
                                dtype=new_dtype, horizon=new_horizon,
                                conviction=new_conviction,
                                feedback=new_feedback if new_feedback else h.get("feedback",""),
                                now=now_str)
                            # changelog
                            _hyp_log = {
                                "timestamp": now_str, "hypothesis_id": hid,
                                "tech_id": h.get("tech_id"), "tech_name": h.get("tech_name"),
                                "from_name": h.get("from_name"), "to_name": h.get("to_name"),
                                "changes": {
                                    "title": new_title, "thesis": new_thesis,
                                    "conviction": new_conviction, "dtype": new_dtype,
                                    "horizon": new_horizon, "companies": new_companies,
                                },
                                "feedback": new_feedback, "edited_by": "human_ui",
                            }
                            try:
                                os.makedirs("data", exist_ok=True)
                                with open("data/hypothesis_changelog.jsonl", "a") as _f:
                                    _f.write(json.dumps(_hyp_log) + "\n")
                            except Exception:
                                pass
                            st.success("Saved")
                            st.session_state[edit_key] = False
                            st.rerun()

            with right_col:
                st.markdown("**Review**")

                if h.get("pending") or status not in ("Validated", "Rejected"):
                    if st.button("✅ Approve", key=f"hyp_approve_{hid}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.pending_human_review = false,
                                n.status = 'Validated',
                                n.reviewed_at = $now,
                                n.reviewed_by = 'human_ui'
                        """, id=hid, now=datetime.now(timezone.utc).isoformat())
                        st.success("✅ Validated")
                        st.rerun()

                    if st.button("⬆️ More research", key=f"hyp_escalate_{hid}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.status = 'Escalated',
                                n.escalated_at = $now
                        """, id=hid, now=datetime.now(timezone.utc).isoformat())
                        st.warning("Sent for more research")
                        st.rerun()

                    if st.button("❌ Reject", key=f"hyp_reject_{hid}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.pending_human_review = false,
                                n.status = 'Rejected',
                                n.reviewed_at = $now,
                                n.reviewed_by = 'human_ui'
                        """, id=hid, now=datetime.now(timezone.utc).isoformat())
                        st.error("❌ Rejected")
                        st.rerun()
                else:
                    st.markdown(f"**Status:** {status_badge} {status}")
                    if st.button("↩️ Reset to pending", key=f"hyp_reset_{hid}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.pending_human_review = true,
                                n.status = 'Hypothesis'
                        """, id=hid, now=datetime.now(timezone.utc).isoformat())
                        st.rerun()

                st.divider()
                st.markdown(f"**Technology**")
                st.markdown(h.get("tech_name",""))
                if activ is not None:
                    st.markdown(f"Activation: **{activ:.3f}**")
                st.markdown(f"**Primary scalar**")
                prim = h.get("primary_scalar") or "—"
                st.markdown(f"`{prim}` {scalar_names.get(prim,'')[:40]}")

# ── Page: Validation Review ───────────────────────────────────────────────────

elif page == "🔬 Validation Review":
    st.title("Validation Review")
    st.caption("Research briefs and adversarial counter-briefs for each hypothesis.")

    validated = run_query("""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
        WITH h, collect({type: ev.evaluation_type, conf: ev.confidence}) AS evals
        RETURN h.hypothesis_id    AS hid,
               h.title            AS title,
               h.thesis           AS thesis,
               h.status           AS status,
               h.conviction_score AS conviction,
               h.validation_score AS validation,
               h.research_confidence AS research_conf,
               h.counter_confidence  AS counter_conf,
               h.companies_actively_transitioning AS transitioning,
               h.from_bim_id AS from_id, h.to_bim_id AS to_id,
               evals
        ORDER BY coalesce(h.validation_score, h.conviction_score) DESC
    """)

    if not validated:
        st.info("No hypotheses yet. Run the extraction pipeline.")
    else:
        for h in validated:
            evals = h.get("evals") or []
            has_research = any(e.get("type") == "deep_research" for e in evals)
            has_counter  = any(e.get("type") == "counter_research" for e in evals)
            val = h.get("validation") or h.get("conviction") or 0
            status = h.get("status", "Hypothesis")
            status_icon = {"Validated": "🟢", "Contested": "🔴",
                           "Hypothesis": "🟡", "Escalated": "⬆️"}.get(status, "⚪")

            with st.expander(
                f"{status_icon} **{h['hid']}** — {h['title'] or 'Untitled'}  "
                f"·  val={val:.2f}  ·  {status}  "
                f"·  {'✅research' if has_research else '❌research'}  "
                f"·  {'✅counter' if has_counter else '❌counter'}"
            ):
                tab_brief, tab_counter, tab_score = st.tabs(
                    ["Research Brief", "Counter Brief", "Validation Score"]
                )

                with tab_brief:
                    if has_research:
                        brief_ev = next((e for e in evals if e.get("type") == "deep_research"), {})
                        st.metric("Research confidence", f"{brief_ev.get('conf', 0):.2f}")
                        driver = get_driver()
                        with driver.session() as s:
                            ev_rec = s.run("""
                                MATCH (ev:Evaluation {evaluation_id:$eid})
                                RETURN ev.content_json AS cj
                            """, eid=f"EVAL_{h['hid']}_DEEP_RESEARCH").single()
                        driver.close()
                        if ev_rec:
                            try:
                                content = json.loads(ev_rec["cj"])
                                st.markdown(f"**Summary:** {content.get('research_summary','')}")
                                st.markdown("**Supporting:**")
                                for e in content.get("supporting_evidence", [])[:4]:
                                    st.markdown(f"- [{e.get('strength','')}] {e.get('claim','')}")
                                st.markdown("**Refuting:**")
                                for e in content.get("refuting_evidence", [])[:3]:
                                    st.markdown(f"- [{e.get('strength','')}] {e.get('claim','')}")
                            except Exception:
                                pass
                    else:
                        st.info("No research brief yet.")
                        if st.button(f"Run deep research", key=f"run_research_{h['hid']}"):
                            from research.deep_researcher import research_hypothesis
                            research_hypothesis(h["hid"])
                            st.rerun()

                with tab_counter:
                    if has_counter:
                        driver = get_driver()
                        with driver.session() as s:
                            ev_rec = s.run("""
                                MATCH (ev:Evaluation {evaluation_id:$eid})
                                RETURN ev.content_json AS cj, ev.confidence AS conf
                            """, eid=f"EVAL_{h['hid']}_COUNTER_RESEARCH").single()
                        driver.close()
                        if ev_rec:
                            st.metric("Adversarial confidence", f"{ev_rec['conf']:.2f}")
                            try:
                                content = json.loads(ev_rec["cj"])
                                st.markdown(f"**Counter thesis:** {content.get('counter_thesis','')}")
                                st.markdown("**Structural barriers:**")
                                for b in content.get("structural_barriers", [])[:4]:
                                    st.markdown(f"- [{b.get('severity','')}] **{b.get('barrier','')}**: {b.get('description','')}")
                            except Exception:
                                pass
                    else:
                        st.info("No counter brief yet.")
                        if st.button(f"Run counter research", key=f"run_counter_{h['hid']}"):
                            from research.deep_researcher import counter_research_hypothesis
                            counter_research_hypothesis(h["hid"])
                            st.rerun()

                with tab_score:
                    cols = st.columns(3)
                    cols[0].metric("Conviction", f"{h.get('conviction') or 0:.2f}")
                    cols[1].metric("Research", f"{h.get('research_conf') or 0:.2f}")
                    cols[2].metric("Validation", f"{val:.2f}")
                    if st.button("Recalculate validation score", key=f"rescore_{h['hid']}"):
                        from research.validation_scorer import score_hypothesis
                        score_hypothesis(h["hid"])
                        st.rerun()


# ── Page: Editorial Queue ─────────────────────────────────────────────────────

elif page == "📝 Editorial Queue":
    st.title("Editorial Queue")
    st.caption(
        "Hypotheses ranked by editorial priority. Add notes, change status, "
        "or promote to deep research."
    )

    priority_filter = st.selectbox(
        "Filter by staleness",
        ["All", "URGENT (no score)", "STALE (new evidence)", "DRIFT (signal changed)", "CURRENT"],
    )

    queue_rows = run_query("""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
        WITH h, v, count(DISTINCT ev) AS eval_count
        RETURN h.hypothesis_id        AS hid,
               h.title                AS title,
               h.status               AS status,
               h.conviction_score     AS conviction,
               h.validation_score     AS validation,
               h.research_confidence  AS research_conf,
               h.counter_confidence   AS counter_conf,
               h.researched_at        AS researched_at,
               h.validated_at         AS validated_at,
               h.signal_at_validation AS signal_at_val,
               h.editorial_note       AS editorial_note,
               h.editorial_priority   AS editorial_priority,
               v.signal_strength      AS signal,
               eval_count
        ORDER BY
            CASE WHEN h.validation_score IS NULL THEN 0 ELSE 1 END ASC,
            h.conviction_score DESC
    """)

    if not queue_rows:
        st.info("No hypotheses in graph yet.")
    else:
        # Apply staleness filter (client-side for simplicity)
        def classify_staleness(row):
            if row.get("validation") is None:
                return "URGENT (no score)"
            researched = row.get("researched_at")
            # We don't have latest_evidence_at in this query; classify remaining as CURRENT
            sig_now  = row.get("signal") or 0
            sig_val  = row.get("signal_at_val") or 0
            if sig_val > 0 and abs(sig_now - sig_val) > 0.10:
                return "DRIFT (signal changed)"
            return "CURRENT"

        if priority_filter != "All":
            queue_rows = [r for r in queue_rows if classify_staleness(r) == priority_filter]

        st.info(f"{len(queue_rows)} hypothesis(es) shown")

        for h in queue_rows:
            staleness = classify_staleness(h)
            stale_icon = {
                "URGENT (no score)":     "🔴",
                "STALE (new evidence)":  "🟡",
                "DRIFT (signal changed)": "🔵",
                "CURRENT":               "🟢",
            }.get(staleness, "⚪")

            val_str = f"{h['validation']:.4f}" if h.get("validation") is not None else "unscored"
            with st.expander(
                f"{stale_icon} **{h['hid']}** — {h['title'] or 'Untitled'}  "
                f"·  {staleness}  ·  conviction={h.get('conviction') or 0:.2f}  "
                f"·  val={val_str}  ·  evals={h.get('eval_count',0)}"
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    metrics_cols = st.columns(4)
                    metrics_cols[0].metric("Conviction", f"{h.get('conviction') or 0:.2f}")
                    metrics_cols[1].metric("Signal",     f"{h.get('signal') or 0:.4f}")
                    metrics_cols[2].metric("Research",   f"{h.get('research_conf') or 0:.2f}")
                    metrics_cols[3].metric("Validation", val_str)

                    # Editorial note
                    current_note = h.get("editorial_note") or ""
                    st.markdown("**Editorial note:**")
                    note_key = f"note_{h['hid']}"
                    new_note = st.text_area(
                        "Add/edit note",
                        value=current_note,
                        key=note_key,
                        height=80,
                        label_visibility="collapsed",
                    )

                    note_col1, note_col2 = st.columns([1, 3])
                    if note_col1.button("💾 Save note", key=f"save_note_{h['hid']}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.editorial_note = $note,
                                n.editorial_updated_at = $now,
                                n.editorial_updated_by = 'human_ui'
                        """, id=h["hid"], note=new_note,
                            now=datetime.now(timezone.utc).isoformat())
                        st.success("Note saved")
                        st.rerun()

                    # Priority label
                    priorities = ["—", "high", "medium", "low", "watch", "archive"]
                    current_pri = h.get("editorial_priority") or "—"
                    current_idx = priorities.index(current_pri) if current_pri in priorities else 0
                    new_pri = st.selectbox(
                        "Editorial priority",
                        priorities,
                        index=current_idx,
                        key=f"pri_{h['hid']}",
                    )
                    if new_pri != current_pri:
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.editorial_priority = $pri
                        """, id=h["hid"], pri=new_pri if new_pri != "—" else None)

                with col2:
                    st.markdown("**Actions**")

                    # Trigger deep research
                    if st.button("🔬 Research now", key=f"ed_research_{h['hid']}"):
                        from research.deep_researcher import (
                            research_hypothesis, counter_research_hypothesis
                        )
                        with st.spinner("Running research..."):
                            research_hypothesis(h["hid"])
                            counter_research_hypothesis(h["hid"])
                        from research.validation_scorer import score_hypothesis
                        score_hypothesis(h["hid"])
                        st.success("Research + scoring complete")
                        st.rerun()

                    # Status change
                    st.markdown("**Set status:**")
                    for new_status, icon in [
                        ("Validated", "✅"), ("Contested", "❌"), ("Hypothesis", "🔄"),
                    ]:
                        if h.get("status") != new_status:
                            if st.button(
                                f"{icon} {new_status}", key=f"ed_set_{h['hid']}_{new_status}"
                            ):
                                run_query("""
                                    MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                                    SET n.status = $status,
                                        n.editorial_status_set_at = $now,
                                        n.editorial_status_set_by = 'human_ui'
                                """, id=h["hid"], status=new_status,
                                    now=datetime.now(timezone.utc).isoformat())
                                st.success(f"Status set to {new_status}")
                                st.rerun()


# ── Page: Pipeline Monitor ────────────────────────────────────────────────────

elif page == "🔄 Pipeline Monitor":
    st.title("Pipeline Monitor")
    st.caption("Run pipeline stages and monitor system health.")

    col_left, col_right = st.columns([2, 1])

    with col_right:
        st.subheader("Quick Actions")
        dry_run_flag = st.checkbox("Dry run (no writes)", value=True)

        if st.button("🔄 Run analysis loop", help="aggregate + trends + rank + monitor + score"):
            from orchestrator.pipeline import run_pipeline
            with st.spinner("Running analysis pipeline..."):
                result = run_pipeline(
                    stages=["aggregate", "trends", "rank", "monitor", "score"],
                    dry_run=dry_run_flag,
                )
            if result["success"]:
                st.success(f"Pipeline complete — {len(result['stages_run'])} stages")
            else:
                st.error(f"Errors: {list(result['errors'].keys())}")
            st.json(result["results"])

        if st.button("📊 Health check"):
            from orchestrator.pipeline import stage_health
            with st.spinner("Checking graph health..."):
                health = stage_health()
            st.json(health)

        if st.button("🔍 Staleness report"):
            from evaluation.monitor import run_monitor
            with st.spinner("Running monitor..."):
                report = run_monitor(drift_threshold=0.10, dry_run=True)
            st.metric("URGENT", report["urgent"])
            st.metric("STALE",  report["stale"])
            st.metric("DRIFT",  report["drift"])
            st.metric("CURRENT", report["current"])
            if report["queue"]:
                st.dataframe([{
                    "ID":       q["hypothesis_id"],
                    "Staleness": q["staleness"],
                    "Reason":   q["staleness_reason"],
                    "Val":      q.get("validation_score"),
                } for q in report["queue"]])

        st.divider()
        if st.button("📸 Take snapshot", help="Save a JSON snapshot of current graph state"):
            try:
                from core.snapshot import take_snapshot
                with st.spinner("Taking snapshot..."):
                    snap = take_snapshot(label="manual")
                if "error" in snap:
                    st.error(snap["error"])
                else:
                    total_nodes = sum(snap.get("node_counts", {}).values())
                    st.success(f"Snapshot saved — {total_nodes} nodes")
            except Exception as e:
                st.error(str(e))

    with col_left:
        tab_stats, tab_errors, tab_history = st.tabs(["📊 Stats", "🔴 Errors", "🕓 History"])

        with tab_stats:
            st.subheader("System Stats")
            stats = run_query("""
                MATCH (n)
                RETURN labels(n)[0] AS label, count(n) AS cnt
                ORDER BY cnt DESC
            """)
            if stats:
                import pandas as pd
                df = pd.DataFrame(stats)
                st.dataframe(df, use_container_width=True)

            st.subheader("Hypothesis Pipeline Status")
            hyp_stats = run_query("""
                MATCH (h:DisruptionHypothesis)
                OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
                WITH h, count(DISTINCT ev) AS eval_count
                RETURN
                    h.status AS status,
                    count(h) AS total,
                    count(h.validation_score) AS scored,
                    count(h.research_confidence) AS researched,
                    sum(eval_count) AS total_evals
            """)
            if hyp_stats:
                st.dataframe(hyp_stats, use_container_width=True)

        with tab_errors:
            try:
                from core.error_log import read_recent, clear_log, error_count_last_24h
                err_24h = error_count_last_24h()
                if err_24h > 0:
                    st.error(f"🔴 {err_24h} error(s) in the last 24 hours")
                else:
                    st.success("✅ No errors in the last 24 hours")

                col_e1, col_e2 = st.columns([3, 1])
                with col_e2:
                    if st.button("🗑 Clear log", help="Truncate the error log file"):
                        clear_log()
                        st.success("Log cleared")
                        st.rerun()

                errors_raw = read_recent(50)
                if not errors_raw:
                    st.info("No errors recorded yet.")
                else:
                    import pandas as pd
                    err_df = pd.DataFrame([{
                        "Time":     r.get("timestamp", "")[:19].replace("T", " "),
                        "Module":   r.get("module", ""),
                        "Function": r.get("function", ""),
                        "Error":    f"{r.get('error_type','')}: {r.get('message','')[:80]}",
                        "Context":  str(r.get("context", "")),
                    } for r in errors_raw])
                    st.dataframe(err_df, use_container_width=True)

                    with st.expander("🔍 Full traceback (last error)"):
                        if errors_raw:
                            st.code(errors_raw[0].get("traceback", ""), language="python")
            except ImportError:
                st.warning("core.error_log not available — run `git init` and restart.")

        with tab_history:
            st.subheader("Code Commits")
            try:
                from core.version_control import get_recent_commits, git_restore, current_sha
                sha_now = current_sha()
                st.caption(f"Current HEAD: `{sha_now}`")

                commits = get_recent_commits(15)
                if not commits:
                    st.info("No commits yet. Run the pipeline once to auto-commit.")
                else:
                    for c in commits:
                        c_col1, c_col2 = st.columns([4, 1])
                        with c_col1:
                            st.markdown(f"**`{c['sha']}`** {c['message']}")
                            st.caption(f"{c['timestamp']} — {c['author']}")
                        with c_col2:
                            if c["sha"] != sha_now:
                                restore_key = f"restore_{c['sha']}"
                                if st.button("⏪ Restore", key=restore_key,
                                             help=f"Restore *.py files to {c['sha']}"):
                                    res = git_restore(c["full_sha"])
                                    if res["restored"]:
                                        st.success(f"Restored to {res['sha']}")
                                    else:
                                        st.error(res["error"])
                        st.divider()
            except ImportError:
                st.warning("core.version_control not available — install GitPython.")

            st.subheader("Data Snapshots")
            try:
                from core.snapshot import list_snapshots
                snapshots = list_snapshots(10)
                if not snapshots:
                    st.info("No snapshots yet. Click '📸 Take snapshot' or run the pipeline.")
                else:
                    for snap in snapshots:
                        with st.expander(
                            f"🗃 {snap['timestamp'][:16]}  ·  {snap['label'] or 'snapshot'}  "
                            f"·  {snap['total_nodes']} nodes"
                        ):
                            s_col1, s_col2, s_col3 = st.columns(3)
                            s_col1.metric("Hypotheses", snap["hypotheses"])
                            s_col2.metric("Vectors", snap["vectors"])
                            s_col3.metric("Evidence", snap["evidence"])
                            st.json(snap["full_data"])
            except ImportError:
                st.warning("core.snapshot not available.")
